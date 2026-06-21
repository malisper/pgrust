//! MCV consume/match leg of `extended_stats.c` + `mcv.c`: the planner-facing
//! `statext_mcv_clauselist_selectivity` and its match engine
//! (`mcv_get_match_bitmap`, the per-item fmgr operator evaluation over the
//! deserialized MCV list).
//!
//! Ported faithfully:
//!   * `statext_is_compatible_clause` / `statext_is_compatible_clause_internal`
//!     (extended_stats.c:1555 / 1328) — the clause-compatibility walk.
//!   * `examine_opclause_args` (extended_stats.c:2032) — the Expr/Const split.
//!   * `stat_find_expression` / `stat_covers_expressions` (extended_stats.c:1138
//!     / 1165) and `choose_best_statistics` (extended_stats.c:1206).
//!   * `mcv_match_expression` (mcv.c:1535) and `mcv_get_match_bitmap`
//!     (mcv.c:1599) — the per-item operator evaluation.
//!   * `statext_mcv_clauselist_selectivity` (extended_stats.c:1693) — the greedy
//!     apply-best-statistics driver.
//!
//! The MCV-list load + deserialize and the frequency-summation kernels
//! (`mcv_clauselist_selectivity`, `mcv_clause_selectivity_or`,
//! `mcv_combine_selectivities`) live in `backend-statistics-mcv`; this module
//! drives them over the planner arena and fmgr dispatch.

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{Const, Expr, NullTestType, AND_EXPR, NOT_EXPR, OR_EXPR};
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{
    JoinType, NodeId, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo, StatisticExtInfo,
};
use types_statistics::{MCVList, STATS_EXT_MCV, STATS_MAX_DIMENSIONS};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_nodes_nodeFuncs_seams as nodefuncs;
use backend_optimizer_path_small_seams as sel_seam;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;

use backend_statistics_mcv as mcv;

use types_core::primitive::Oid;

use crate::estimate::{clamp_probability, strip_relabel};

/// Selectivity-estimator pg_proc OIDs the MCV estimator accepts (pg_proc.dat).
const F_EQSEL: Oid = 101;
const F_NEQSEL: Oid = 102;
const F_SCALARLTSEL: Oid = 103;
const F_SCALARLESEL: Oid = 336;
const F_SCALARGTSEL: Oid = 104;
const F_SCALARGESEL: Oid = 337;

/// `BMS_SINGLETON` (bitmapset.h) — a `bms_membership` return value.
const BMS_SINGLETON: i32 = 1;

/// `RESULT_MERGE(value, is_or, match)` (mcv.c:84).
#[inline]
const fn result_merge(value: bool, is_or: bool, m: bool) -> bool {
    if is_or {
        value || m
    } else {
        value && m
    }
}

/// `RESULT_IS_FINAL(value, is_or)` (mcv.c:100).
#[inline]
const fn result_is_final(value: bool, is_or: bool) -> bool {
    if is_or {
        value
    } else {
        !value
    }
}

/// `is_opclause(node)` (nodeFuncs.h:78) — `IsA(clause, OpExpr)` only (NOT
/// DistinctExpr/NullIfExpr).
#[inline]
fn is_opclause(node: &Expr) -> bool {
    matches!(node, Expr::OpExpr(_))
}

#[inline]
fn is_andclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == AND_EXPR)
}

#[inline]
fn is_orclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == OR_EXPR)
}

#[inline]
fn is_notclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == NOT_EXPR)
}

/// `AttrNumberIsForUserDefinedAttr(attnum)` — `attnum > 0`.
#[inline]
fn attr_is_user_defined(attnum: i32) -> bool {
    attnum > 0
}

/// `bms_member_index(keys, attnum)` (bitmapset.c): the 0-based index of `attnum`
/// among the set members (in ascending order), or -1 if absent. Faithful to the
/// C: count members strictly below `attnum`, then verify membership.
fn bms_member_index(keys: &Relids, attnum: i32) -> i32 {
    if !bms::relids_is_member::call(attnum, keys) {
        return -1;
    }
    let mut idx = 0i32;
    let mut prev = -1i32;
    loop {
        let m = bms::relids_next_member::call(keys, prev);
        if m < 0 {
            // Should not happen given the membership check above.
            return -1;
        }
        if m == attnum {
            return idx;
        }
        idx += 1;
        prev = m;
    }
}

/* ===========================================================================
 * examine_opclause_args (extended_stats.c:2032)
 * ======================================================================== */

/// `examine_opclause_args(args, &expr, &cst, &expronleft)` (extended_stats.c:2032)
/// — split a 2-arg operator's arguments into the (Expr, Const, expronleft) parts,
/// stripping a RelabelType off either side. Returns `None` when neither side is a
/// bare `Const` (the C `false` return).
fn examine_opclause_args<'a, 'mcx>(
    args: &'a [Expr<'mcx>],
) -> Option<(&'a Expr<'mcx>, &'a Const<'mcx>, bool)> {
    // Assert(list_length(args) == 2) — enforced by the caller.
    if args.len() != 2 {
        return None;
    }
    let leftop = strip_relabel(&args[0]);
    let rightop = strip_relabel(&args[1]);

    if let Expr::Const(cst) = rightop {
        // expr op Const
        Some((leftop, cst, true))
    } else if let Expr::Const(cst) = leftop {
        // Const op expr
        Some((rightop, cst, false))
    } else {
        None
    }
}

/// Whether `get_oprrest(opno)` is one of the MCV-supported estimators.
fn oprrest_supported(opno: Oid) -> PgResult<bool> {
    let rest = lsyscache::get_oprrest::call(opno)?;
    Ok(matches!(
        rest,
        F_EQSEL | F_NEQSEL | F_SCALARLTSEL | F_SCALARLESEL | F_SCALARGTSEL | F_SCALARGESEL
    ))
}

/* ===========================================================================
 * statext_is_compatible_clause_internal (extended_stats.c:1328)
 * ======================================================================== */

fn statext_is_compatible_clause_internal(
    root: &PlannerInfo,
    clause: &Expr,
    relid: i32,
    attnums: &mut Relids,
    exprs: &mut Vec<Expr>,
    leakproof: &mut bool,
    run: &PlannerRun<'_>,
) -> PgResult<bool> {
    // Look inside any binary-compatible relabeling.
    let clause = strip_relabel(clause);

    // plain Var references.
    if let Expr::Var(var) = clause {
        if var.varno != relid {
            return Ok(false);
        }
        if var.varlevelsup > 0 {
            return Ok(false);
        }
        if !attr_is_user_defined(var.varattno as i32) {
            return Ok(false);
        }
        *attnums = bms::relids_add_member::call(attnums.take(), var.varattno as i32);
        return Ok(true);
    }

    // (Var/Expr op Const) or (Const op Var/Expr).
    if is_opclause(clause) {
        let expr = clause.as_opexpr().expect("is_opclause => OpExpr payload");
        if expr.args.len() != 2 {
            return Ok(false);
        }
        let clause_expr = match examine_opclause_args(&expr.args) {
            Some((ce, _cst, _onleft)) => ce,
            None => return Ok(false),
        };
        if !oprrest_supported(expr.opno)? {
            return Ok(false);
        }
        if *leakproof {
            *leakproof = lsyscache::get_func_leakproof::call(lsyscache::get_opcode::call(expr.opno)?)?;
        }
        if let Expr::Var(_) = clause_expr {
            return statext_is_compatible_clause_internal(
                root, clause_expr, relid, attnums, exprs, leakproof, run,
            );
        }
        exprs.push(clause_expr.clone_in(run.mcx())?);
        return Ok(true);
    }

    // Var/Expr IN Array.
    if let Expr::ScalarArrayOpExpr(expr) = clause {
        if expr.args.len() != 2 {
            return Ok(false);
        }
        let (clause_expr, _cst, expronleft) = match examine_opclause_args(&expr.args) {
            Some(t) => t,
            None => return Ok(false),
        };
        // We only support Var on left, Const on right.
        if !expronleft {
            return Ok(false);
        }
        if !oprrest_supported(expr.opno)? {
            return Ok(false);
        }
        if *leakproof {
            *leakproof = lsyscache::get_func_leakproof::call(lsyscache::get_opcode::call(expr.opno)?)?;
        }
        if let Expr::Var(_) = clause_expr {
            return statext_is_compatible_clause_internal(
                root, clause_expr, relid, attnums, exprs, leakproof, run,
            );
        }
        exprs.push(clause_expr.clone_in(run.mcx())?);
        return Ok(true);
    }

    // AND/OR/NOT clause.
    if is_andclause(clause) || is_orclause(clause) || is_notclause(clause) {
        let bexpr = clause.as_boolexpr().expect("AND/OR/NOT => BoolExpr payload");
        for arg in &bexpr.args {
            if !statext_is_compatible_clause_internal(
                root, arg, relid, attnums, exprs, leakproof, run,
            )? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    // Var/Expr IS NULL.
    if let Expr::NullTest(nt) = clause {
        let arg: &Expr = nt
            .arg
            .as_deref()
            .expect("NullTest with NULL arg is malformed");
        if let Expr::Var(_) = arg {
            return statext_is_compatible_clause_internal(
                root, arg, relid, attnums, exprs, leakproof, run,
            );
        }
        exprs.push(arg.clone_in(run.mcx())?);
        return Ok(true);
    }

    // Any other expression: a bare expression to match against statistics exprs.
    exprs.push(clause.clone_in(run.mcx())?);
    Ok(true)
}

/* ===========================================================================
 * statext_is_compatible_clause (extended_stats.c:1555)
 * ======================================================================== */

/// `statext_is_compatible_clause(root, clause, relid, &attnums, &exprs)`
/// (extended_stats.c:1555). `clause` is a (possibly bare-AND) clause; the
/// RestrictInfo superstructure is unwrapped here. The leakproof permission check
/// is faithfully ported: for the MCV-supported operators (=, <>, <, <=, >, >=)
/// every operator is leakproof, so `leakproof` stays true and the per-column
/// permission branch is never entered. If a non-leakproof operator ever reaches
/// this path, the permission check is enforced via the (currently unported)
/// pull_varattnos/all_rows_selectable owners — until they land, such a clause is
/// conservatively rejected (returns false), exactly the safe direction the C
/// permission check would take when access is not provable.
fn statext_is_compatible_clause(
    root: &PlannerInfo,
    clause: &Expr,
    relid: i32,
    attnums: &mut Relids,
    exprs: &mut Vec<Expr>,
    run: &PlannerRun<'_>,
) -> PgResult<bool> {
    // Special-case bare BoolExpr AND clauses (no RestrictInfo built on top).
    if is_andclause(clause) {
        let bexpr = clause.as_boolexpr().expect("is_andclause => BoolExpr");
        for arg in &bexpr.args {
            if !statext_is_compatible_clause(root, arg, relid, attnums, exprs, run)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    // This entry is only reached with an already-unwrapped clause Expr; the
    // RestrictInfo pseudoconstant/singleton checks are applied by the driver's
    // per-clause loop (see statext_mcv_clauselist_selectivity), mirroring the FD
    // path's structure where the rinfo guards live in the driver.
    let mut leakproof = true;
    if !statext_is_compatible_clause_internal(
        root,
        clause,
        relid,
        attnums,
        exprs,
        &mut leakproof,
        run,
    )? {
        return Ok(false);
    }

    if !leakproof {
        // The permission check (pull_varattnos + all_rows_selectable) owner is
        // not yet ported. Reject conservatively rather than expose values the
        // user may not be permitted to read.
        return Ok(false);
    }

    Ok(true)
}

/* ===========================================================================
 * stat_find_expression / stat_covers_expressions (extended_stats.c:1138/1165)
 * ======================================================================== */

fn stat_find_expression(stat: &StatisticExtInfo, expr: &Expr, root: &PlannerInfo) -> i32 {
    let mut idx = 0i32;
    for &eid in &stat.exprs {
        let stat_expr = root.node(eid);
        if nodefuncs::equal::call(stat_expr, expr) {
            return idx;
        }
        idx += 1;
    }
    -1
}

fn stat_covers_expressions(
    stat: &StatisticExtInfo,
    exprs: &[Expr],
    expr_idxs: Option<&mut Relids>,
    root: &PlannerInfo,
) -> bool {
    let mut idxs: Relids = None;
    for expr in exprs {
        let expr_idx = stat_find_expression(stat, expr, root);
        if expr_idx == -1 {
            return false;
        }
        idxs = bms::relids_add_member::call(idxs.take(), expr_idx);
    }
    if let Some(out) = expr_idxs {
        *out = bms::relids_add_members::call(out.take(), &idxs);
    }
    true
}

/* ===========================================================================
 * choose_best_statistics (extended_stats.c:1206)
 * ======================================================================== */

/// Returns the chosen statistics object's `statlist` NodeId, or None.
fn choose_best_statistics(
    root: &PlannerInfo,
    rel: RelId,
    requiredkind: i8,
    inh: bool,
    clause_attnums: &[Relids],
    clause_exprs: &[Vec<Expr>],
    nclauses: usize,
) -> Option<NodeId> {
    let mut best_match: Option<NodeId> = None;
    let mut best_num_matched = 2; // goal #1: maximize
    let mut best_match_keys = (STATS_MAX_DIMENSIONS as i32) + 1; // goal #2: minimize

    let statlist = root.rel(rel).statlist.clone();
    for id in statlist {
        let info = root.statistic_ext(id);

        if info.kind != requiredkind {
            continue;
        }
        if info.inherit != inh {
            continue;
        }

        let mut matched_attnums: Relids = None;
        let mut matched_exprs: Relids = None;

        for i in 0..nclauses {
            // ignore incompatible/estimated clauses
            if clause_attnums[i].is_none() && clause_exprs[i].is_empty() {
                continue;
            }
            // ignore clauses not covered by this object
            let mut expr_idxs: Relids = None;
            if !bms::relids_is_subset::call(&clause_attnums[i], &info.keys)
                || !stat_covers_expressions(info, &clause_exprs[i], Some(&mut expr_idxs), root)
            {
                continue;
            }
            matched_attnums =
                bms::relids_add_members::call(matched_attnums.take(), &clause_attnums[i]);
            matched_exprs = bms::relids_add_members::call(matched_exprs.take(), &expr_idxs);
        }

        let num_matched =
            bms::relids_num_members::call(&matched_attnums) + bms::relids_num_members::call(&matched_exprs);

        let numkeys =
            bms::relids_num_members::call(&info.keys) + info.exprs.len() as i32;

        if num_matched > best_num_matched
            || (num_matched == best_num_matched && numkeys < best_match_keys)
        {
            best_match = Some(id);
            best_num_matched = num_matched;
            best_match_keys = numkeys;
        }
    }

    best_match
}

/* ===========================================================================
 * mcv_match_expression (mcv.c:1535)
 * ======================================================================== */

/// `mcv_match_expression(expr, keys, exprs, &collid)` (mcv.c:1535) — match the
/// attribute/expression operand to a dimension index of the statistic, also
/// returning the collation to use. `keys` is the stat's covered attnums; `exprs`
/// are the stat's covered expressions (after the simple columns).
fn mcv_match_expression(
    expr: &Expr,
    keys: &Relids,
    exprs: &[Expr],
    want_collid: bool,
) -> PgResult<(i32, Oid)> {
    if let Expr::Var(var) = expr {
        let collid = if want_collid { var.varcollid } else { 0 };
        let idx = bms_member_index(keys, var.varattno as i32);
        if idx < 0 {
            return Err(PgError::error(
                "variable not found in statistics object",
            ));
        }
        Ok((idx, collid))
    } else {
        let collid = if want_collid {
            nodefuncs::exprCollation::call(expr)
        } else {
            0
        };
        // expressions are stored after the simple columns.
        let mut idx = bms::relids_num_members::call(keys);
        let mut found = false;
        for stat_expr in exprs {
            if nodefuncs::equal::call(expr, stat_expr) {
                found = true;
                break;
            }
            idx += 1;
        }
        if !found {
            return Err(PgError::error(
                "expression not found in statistics object",
            ));
        }
        Ok((idx, collid))
    }
}

/* ===========================================================================
 * mcv_get_match_bitmap (mcv.c:1599)
 * ======================================================================== */

/// `mcv_get_match_bitmap(root, clauses, keys, exprs, mcvlist, is_or)`
/// (mcv.c:1599) — evaluate the clause list against the MCV list and return a
/// per-item match bitmap (length `mcvlist->nitems`). `clauses` are bare clause
/// Exprs (RestrictInfo already unwrapped); `keys`/`exprs` come from the chosen
/// statistic.
fn mcv_get_match_bitmap(
    mcx: Mcx<'_>,
    clauses: &[Expr],
    keys: &Relids,
    exprs: &[Expr],
    mcvlist: &MCVList,
    is_or: bool,
) -> PgResult<Vec<bool>> {
    let nitems = mcvlist.nitems as usize;
    let mut matches = alloc::vec![!is_or; nitems];

    for clause in clauses {
        // if it's a RestrictInfo, extract the clause — we already hold bare Exprs.
        let clause = clause;

        if is_opclause(clause) {
            let expr = clause.as_opexpr().expect("is_opclause => OpExpr");
            let opfunc = lsyscache::get_opcode::call(expr.opno)?;

            let (clause_expr, cst, expronleft) = match examine_opclause_args(&expr.args) {
                Some(t) => t,
                None => return Err(PgError::error("incompatible clause")),
            };
            let (idx, collid) = mcv_match_expression(clause_expr, keys, exprs, true)?;

            for i in 0..nitems {
                let item = &mcvlist.items[i];
                // NULL item value or NULL Const => mismatch (strictness).
                if item.isnull[idx as usize] || cst.constisnull {
                    matches[i] = result_merge(matches[i], is_or, false);
                    continue;
                }
                if result_is_final(matches[i], is_or) {
                    continue;
                }
                let item_val = item.values[idx as usize].clone_in(mcx)?;
                let cst_val = cst.constvalue.clone_in(mcx)?;
                let result = if expronleft {
                    fmgr::function_call2_coll_datum::call(mcx, opfunc, collid, item_val, cst_val)?
                } else {
                    fmgr::function_call2_coll_datum::call(mcx, opfunc, collid, cst_val, item_val)?
                };
                let m = result.as_bool();
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if let Expr::ScalarArrayOpExpr(expr) = clause {
            let opfunc = lsyscache::get_opcode::call(expr.opno)?;

            let (clause_expr, cst, expronleft) = match examine_opclause_args(&expr.args) {
                Some(t) => t,
                None => return Err(PgError::error("incompatible clause")),
            };
            // We expect Var on left.
            if !expronleft {
                return Err(PgError::error("incompatible clause"));
            }

            // Deconstruct the array constant, unless it's NULL.
            let mut elem_values: Vec<Datum> = Vec::new();
            let mut elem_nulls: Vec<bool> = Vec::new();
            if !cst.constisnull {
                let arr = cst.constvalue.as_ref_bytes();
                let elmtype = backend_utils_adt_arrayfuncs::foundation::arr_elemtype(arr);
                let tlba = lsyscache::get_typlenbyvalalign::call(elmtype)?;
                let elems = backend_utils_adt_arrayfuncs::construct::deconstruct_array_values(
                    mcx,
                    arr,
                    elmtype,
                    tlba.typlen as i32,
                    tlba.typbyval,
                    tlba.typalign as u8,
                )?;
                for (d, isn) in elems.iter() {
                    elem_values.push(d.clone_in(mcx)?);
                    elem_nulls.push(*isn);
                }
            }
            let num_elems = elem_values.len();

            let (idx, collid) = mcv_match_expression(clause_expr, keys, exprs, true)?;

            for i in 0..nitems {
                let item = &mcvlist.items[i];
                let mut m = !expr.useOr;

                if item.isnull[idx as usize] || cst.constisnull {
                    matches[i] = result_merge(matches[i], is_or, false);
                    continue;
                }
                if result_is_final(matches[i], is_or) {
                    continue;
                }

                for j in 0..num_elems {
                    if elem_nulls[j] {
                        m = result_merge(m, expr.useOr, false);
                        continue;
                    }
                    if result_is_final(m, expr.useOr) {
                        break;
                    }
                    let item_val = item.values[idx as usize].clone_in(mcx)?;
                    let elem_val = elem_values[j].clone_in(mcx)?;
                    let elem_match = fmgr::function_call2_coll_datum::call(
                        mcx, opfunc, collid, item_val, elem_val,
                    )?
                    .as_bool();
                    m = result_merge(m, expr.useOr, elem_match);
                }

                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if let Expr::NullTest(expr) = clause {
            let clause_expr: &Expr = expr
                .arg
                .as_deref()
                .expect("NullTest with NULL arg is malformed");
            let (idx, _collid) = mcv_match_expression(clause_expr, keys, exprs, false)?;

            for i in 0..nitems {
                let item = &mcvlist.items[i];
                let mut m = false;
                match expr.nulltesttype {
                    NullTestType::IS_NULL => {
                        if item.isnull[idx as usize] {
                            m = true;
                        }
                    }
                    NullTestType::IS_NOT_NULL => {
                        if !item.isnull[idx as usize] {
                            m = true;
                        }
                    }
                }
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if is_orclause(clause) || is_andclause(clause) {
            let bexpr = clause.as_boolexpr().expect("AND/OR => BoolExpr");
            let bool_matches = mcv_get_match_bitmap(
                mcx,
                &bexpr.args,
                keys,
                exprs,
                mcvlist,
                is_orclause(clause),
            )?;
            for i in 0..nitems {
                matches[i] = result_merge(matches[i], is_or, bool_matches[i]);
            }
        } else if is_notclause(clause) {
            let bexpr = clause.as_boolexpr().expect("NOT => BoolExpr");
            let not_matches =
                mcv_get_match_bitmap(mcx, &bexpr.args, keys, exprs, mcvlist, false)?;
            for i in 0..nitems {
                matches[i] = result_merge(matches[i], is_or, !not_matches[i]);
            }
        } else if let Expr::Var(var) = clause {
            // boolean Var (possibly from below NOT)
            let idx = bms_member_index(keys, var.varattno as i32);
            if idx < 0 {
                return Err(PgError::error(
                    "variable not found in statistics object",
                ));
            }
            for i in 0..nitems {
                let item = &mcvlist.items[i];
                let mut m = false;
                if !item.isnull[idx as usize] && item.values[idx as usize].as_bool() {
                    m = true;
                }
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else {
            // bare boolean-returning expression
            let (idx, _collid) = mcv_match_expression(clause, keys, exprs, false)?;
            for i in 0..nitems {
                let item = &mcvlist.items[i];
                let m = !item.isnull[idx as usize] && item.values[idx as usize].as_bool();
                matches[i] = result_merge(matches[i], is_or, m);
            }
        }
    }

    Ok(matches)
}

/* ===========================================================================
 * statext_mcv_clauselist_selectivity (extended_stats.c:1693)
 * ======================================================================== */

/// `statext_mcv_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo,
/// rel, &estimatedclauses, is_or)` (extended_stats.c:1693). Drives the greedy
/// apply-best-MCV-statistics loop, returning the partial selectivity and updating
/// `estimatedclauses` in place.
#[allow(clippy::too_many_arguments)]
pub fn statext_mcv_clauselist_selectivity(
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    rel: RelId,
    estimatedclauses: &mut Relids,
    is_or: bool,
) -> PgResult<f64> {
    let mut sel: f64 = if is_or { 0.0 } else { 1.0 };

    if !crate::estimate::has_stats_of_kind(root, rel, STATS_EXT_MCV) {
        return Ok(sel);
    }

    let rel_relid = root.rel(rel).relid;
    let rte_inh = planner_rt_fetch(run, root, rel_relid).inh;

    let nclauses = clauses.len();

    // Pre-process the clause list: extract attnums and expressions per clause.
    // A clause is bare-cloned from its RestrictInfo (the rinfo guards — pseudo-
    // constant, single-rel — are applied here, matching statext_is_compatible_
    // clause's RestrictInfo layer).
    let mut list_attnums: Vec<Relids> = Vec::with_capacity(nclauses);
    let mut list_exprs: Vec<Vec<Expr>> = Vec::with_capacity(nclauses);
    // Keep the cloned bare clause Exprs around (the match bitmap reads them).
    let mut clause_nodes: Vec<Expr> = Vec::with_capacity(nclauses);

    for (listidx, &rid) in clauses.iter().enumerate() {
        let clause: Expr = {
            let rinfo = root.rinfo(rid);
            root.node(rinfo.clause).clone_in(run.mcx())?
        };

        let mut attnums: Relids = None;
        let mut exprs: Vec<Expr> = Vec::new();

        let already = bms::relids_is_member::call(listidx as i32, estimatedclauses);
        if !already {
            // The RestrictInfo guards (pseudoconstant / single-rel) live in
            // statext_is_compatible_clause's RestrictInfo layer; apply them here.
            let rinfo = root.rinfo(rid);
            let pseudoconstant = rinfo.pseudoconstant;
            let single_rel =
                bms::relids_get_singleton_member::call(&rinfo.clause_relids) == Some(rel_relid as i32);
            let compatible = if pseudoconstant || !single_rel {
                false
            } else {
                statext_is_compatible_clause(
                    root,
                    &clause,
                    rel_relid as i32,
                    &mut attnums,
                    &mut exprs,
                    run,
                )?
            };
            if !compatible {
                attnums = None;
                exprs = Vec::new();
            }
        }

        list_attnums.push(attnums);
        list_exprs.push(exprs);
        clause_nodes.push(clause);
    }

    // Apply as many extended statistics as possible.
    loop {
        let stat_id = match choose_best_statistics(
            root,
            rel,
            STATS_EXT_MCV,
            rte_inh,
            &list_attnums,
            &list_exprs,
            nclauses,
        ) {
            Some(id) => id,
            None => break,
        };

        // Snapshot the chosen statistic's identity (keys/exprs/oid) so we can
        // mutate `root` (estimatedclauses) and call seams without aliasing.
        let stat_oid;
        let stat_keys: Relids;
        let stat_exprs: Vec<Expr>;
        {
            let stat = root.statistic_ext(stat_id);
            stat_oid = stat.stat_oid;
            stat_keys = stat.keys.clone();
            let mut se = Vec::with_capacity(stat.exprs.len());
            for &eid in &stat.exprs {
                se.push(root.node(eid).clone_in(run.mcx())?);
            }
            stat_exprs = se;
        }

        // Filter the clauses to estimate with this MCV; track simple clauses.
        // `stat_clauses` holds the bare clause Exprs (the match bitmap reads
        // them); `stat_rinfos` the matching RestrictInfo handles (the simple-
        // selectivity seams estimate over the RestrictInfo, like C).
        let mut stat_clauses: Vec<Expr> = Vec::new();
        let mut stat_rinfos: Vec<RinfoId> = Vec::new();
        let mut simple_clauses: Relids = None;

        for listidx in 0..nclauses {
            if list_attnums[listidx].is_none() && list_exprs[listidx].is_empty() {
                continue;
            }
            let mut expr_idxs: Relids = None;
            if !bms::relids_is_subset::call(&list_attnums[listidx], &stat_keys)
                || !stat_covers_expressions(
                    root.statistic_ext(stat_id),
                    &list_exprs[listidx],
                    Some(&mut expr_idxs),
                    root,
                )
            {
                continue;
            }

            // record simple clauses (single column or single expression)
            let is_simple = (list_attnums[listidx].is_none()
                && list_exprs[listidx].len() == 1)
                || (list_exprs[listidx].is_empty()
                    && bms::relids_membership::call(&list_attnums[listidx]) == BMS_SINGLETON);
            if is_simple {
                simple_clauses =
                    bms::relids_add_member::call(simple_clauses.take(), stat_clauses.len() as i32);
            }

            stat_clauses.push(clause_nodes[listidx].clone_in(run.mcx())?);
            stat_rinfos.push(clauses[listidx]);
            *estimatedclauses =
                bms::relids_add_member::call(estimatedclauses.take(), listidx as i32);

            // Reset the pointers so choose_best_statistics skips this clause.
            list_attnums[listidx] = None;
            list_exprs[listidx] = Vec::new();
        }

        // Load the MCV list once.
        let mcvlist = match mcv::statext_mcv_load(run.mcx(), stat_oid, rte_inh)? {
            Some(m) => m,
            None => {
                return Err(PgError::error(
                    "MCV list not built for statistics object",
                ))
            }
        };

        if is_or {
            let mut or_matches: Vec<bool> = Vec::new();
            let mut simple_or_sel: f64 = 0.0;
            let mut stat_sel: f64 = 0.0;

            for (listidx, clause) in stat_clauses.iter().enumerate() {
                // simple selectivity of this single clause, with
                // use_extended_stats=false (so it cannot recursively re-enter
                // extended statistics). clauselist_selectivity of a single
                // RestrictInfo equals clause_selectivity of it.
                let one_rinfo = [stat_rinfos[listidx]];
                let simple_sel = sel_seam::clauselist_selectivity_ext::call(
                    run,
                    root,
                    &one_rinfo,
                    var_relid,
                    jointype,
                    sjinfo,
                    false,
                )?;

                let overlap_simple_sel = simple_or_sel * simple_sel;
                simple_or_sel += simple_sel - overlap_simple_sel;
                simple_or_sel = clamp_probability(simple_or_sel);

                // per-clause match bitmap (list_make1(clause)).
                let one = [clause.clone_in(run.mcx())?];
                let new_matches =
                    mcv_get_match_bitmap(run.mcx(), &one, &stat_keys, &stat_exprs, &mcvlist, false)?;

                let or = mcv::mcv_clause_selectivity_or(
                    run.mcx(),
                    &mcvlist,
                    &new_matches,
                    &mut or_matches,
                )?;

                let clause_sel = if bms::relids_is_member::call(listidx as i32, &simple_clauses) {
                    simple_sel
                } else {
                    mcv::mcv_combine_selectivities(simple_sel, or.s, or.basesel, or.totalsel)
                };

                let overlap_sel = mcv::mcv_combine_selectivities(
                    overlap_simple_sel,
                    or.overlap_mcvsel,
                    or.overlap_basesel,
                    or.totalsel,
                );

                stat_sel += clause_sel - overlap_sel;
                stat_sel = clamp_probability(stat_sel);
            }

            sel = sel + stat_sel - sel * stat_sel;
        } else {
            // Implicitly-ANDed list of clauses.
            let simple_sel = sel_seam::clauselist_selectivity_ext::call(
                run,
                root,
                &stat_rinfos,
                var_relid,
                jointype,
                sjinfo,
                false,
            )?;

            let matches =
                mcv_get_match_bitmap(run.mcx(), &stat_clauses, &stat_keys, &stat_exprs, &mcvlist, false)?;
            let cl = mcv::mcv_clauselist_selectivity(&mcvlist, &matches);

            let stat_sel =
                mcv::mcv_combine_selectivities(simple_sel, cl.s, cl.basesel, cl.totalsel);

            sel *= stat_sel;
        }
    }

    Ok(sel)
}
