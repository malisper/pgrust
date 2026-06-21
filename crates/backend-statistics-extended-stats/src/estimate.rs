//! Estimate / consume side of `extended_stats.c` + `dependencies.c`: the
//! planner-facing entry `statext_clauselist_selectivity`, which applies the
//! extended statistics built by ANALYZE to refine a clause-list selectivity.
//!
//! Ported faithfully (extended_stats.c:1981, dependencies.c:741-1829):
//!   * `statext_clauselist_selectivity` — the top-level entry (MCV leg first,
//!     then functional dependencies on the remaining clauses).
//!   * `dependencies_clauselist_selectivity` — the functional-dependency driver
//!     (extract per-clause attnums, load matching FD stats, greedily pick the
//!     strongest dependencies, combine their conditional probabilities).
//!   * `dependency_is_compatible_clause` — the per-clause compatibility test.
//!   * `clauselist_apply_dependencies` — the selectivity-combination driver
//!     (per-attribute simple selectivity via the clausesel seam, then the
//!     conditional-probability kernel in `backend-statistics-dependencies`).
//!   * `statext_dependencies_load` — the `pg_statistic_ext_data` syscache load
//!     of the serialized dependencies bytea (over the table_open/genam/heaptuple
//!     substrate this crate already uses for `statext_store`).
//!   * `examine_opclause_args` (extended_stats.c:2032) — the Expr/Const split.
//!
//! The reusable kernels (`find_strongest_dependency`,
//! `combine_dependency_selectivities`, `statext_dependencies_deserialize`) live
//! in `backend-statistics-dependencies`; this module drives them over the
//! planner arena (the `PlannerInfo`/`RelOptInfo`/`RestrictInfo` Node model and
//! the clausesel/lsyscache seams).
//!
//! The MCV leg (`statext_mcv_clauselist_selectivity`) and its match engine
//! (`mcv_get_match_bitmap`, fmgr per-item operator evaluation over the
//! deserialized MCV list) are a separate, larger fmgr-coupled body owned by the
//! MCV crate's match dispatcher; until it lands, the MCV leg contributes its
//! neutral identity (1.0 for AND, 0.0 for OR), exactly as a relation with no MCV
//! statistics would (`has_stats_of_kind(.., STATS_EXT_MCV) == false`).

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{Expr, NOT_EXPR, OR_EXPR};
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{Bitmapset, JoinType, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};

use types_statistics::{MVDependencies, MVDependency, STATS_EXT_DEPENDENCIES};

use types_catalog::pg_statistic_ext::{
    Anum_pg_statistic_ext_data_stxddependencies, Anum_pg_statistic_ext_data_stxdinherit,
    Anum_pg_statistic_ext_data_stxoid, StatisticExtDataRelationId,
    StatisticExtDataStxoidInhIndexId,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::AccessShareLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::table_open;
use backend_statistics_dependencies as deps;

use backend_nodes_nodeFuncs_seams as nodefuncs;
use backend_optimizer_path_small_seams as sel_seam;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as lsyscache;

use types_core::fmgr::F_OIDEQ;
use types_core::primitive::Oid;

/// `get_oprrest(F_EQSEL)` — the equality-selectivity estimator's pg_proc OID
/// (pg_proc.dat: `eqsel` = 101). dependencies.c only accepts `=`.
const F_EQSEL: Oid = 101;

/// `InvalidAttrNumber` (0).
const INVALID_ATTNUM: i32 = 0;

/// `BMS_SINGLETON` / `BMS_MULTIPLE` (bitmapset.h) — the `bms_membership` returns.
const BMS_SINGLETON: i32 = 1;
const BMS_MULTIPLE: i32 = 2;

/// `AttrNumberIsForUserDefinedAttr(attnum)` — `attnum > 0`.
#[inline]
fn attr_is_user_defined(attnum: i32) -> bool {
    attnum > 0
}

/// `CLAMP_PROBABILITY(p)` — clamp to [0,1].
#[inline]
fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

/* ===========================================================================
 * examine_opclause_args (extended_stats.c:2032)
 * ======================================================================== */

/// `if (IsA(node, RelabelType)) node = ((RelabelType *) node)->arg` — strip one
/// binary-compatible relabel (the C arg is never NULL for a valid RelabelType).
#[inline]
fn strip_relabel(node: &Expr) -> &Expr {
    match node {
        Expr::RelabelType(r) => match &r.arg {
            Some(inner) => inner,
            None => node,
        },
        other => other,
    }
}

/* ===========================================================================
 * dependency_is_compatible_clause (dependencies.c:741)
 *
 * Decide whether a (RestrictInfo-unwrapped) clause is a simple Var-equality
 * usable by functional dependencies, returning the Var's attnum.
 * ======================================================================== */

fn dependency_is_compatible_clause(clause: &Expr, relid: i32) -> PgResult<Option<i32>> {
    let clause_expr: &Expr = match clause {
        Expr::OpExpr(expr) | Expr::DistinctExpr(expr) | Expr::NullIfExpr(expr) => {
            // Var = Const or Const = Var.
            if expr.args.len() != 2 {
                return Ok(None);
            }
            // Make sure the non-selected argument is a pseudoconstant.
            let cexpr = if sel_seam::is_pseudo_constant_clause::call(&expr.args[1])? {
                &expr.args[0]
            } else if sel_seam::is_pseudo_constant_clause::call(&expr.args[0])? {
                &expr.args[1]
            } else {
                return Ok(None);
            };
            if lsyscache::get_oprrest::call(expr.opno)? != F_EQSEL {
                return Ok(None);
            }
            cexpr
        }
        Expr::ScalarArrayOpExpr(expr) => {
            // Var IN Const: reject ALL(), require 2 args, second pseudoconstant.
            if !expr.useOr {
                return Ok(None);
            }
            if expr.args.len() != 2 {
                return Ok(None);
            }
            if !sel_seam::is_pseudo_constant_clause::call(&expr.args[1])? {
                return Ok(None);
            }
            if lsyscache::get_oprrest::call(expr.opno)? != F_EQSEL {
                return Ok(None);
            }
            &expr.args[0]
        }
        Expr::BoolExpr(b) if b.boolop == OR_EXPR => {
            // OR: all arguments must reference the same compatible attnum.
            let mut attnum = INVALID_ATTNUM;
            for arg in &b.args {
                match dependency_is_compatible_clause(arg, relid)? {
                    None => return Ok(None),
                    Some(clause_attnum) => {
                        if attnum == INVALID_ATTNUM {
                            attnum = clause_attnum;
                        }
                        if attnum != clause_attnum {
                            return Ok(None);
                        }
                    }
                }
            }
            return Ok(Some(attnum));
        }
        Expr::BoolExpr(b) if b.boolop == NOT_EXPR => {
            // "NOT x" == "x = false": check the argument as a Var.
            nodefuncs::get_notclausearg::call(clause)
        }
        _ => {
            // A bare boolean expression "x" == "x = true": check it as a Var.
            clause
        }
    };

    // Ignore any RelabelType above the operand.
    let clause_expr = strip_relabel(clause_expr);

    // We only support plain Vars.
    let var = match clause_expr {
        Expr::Var(v) => v,
        _ => return Ok(None),
    };

    if var.varno != relid {
        return Ok(None);
    }
    if var.varlevelsup > 0 {
        return Ok(None);
    }
    if !attr_is_user_defined(var.varattno as i32) {
        return Ok(None);
    }

    Ok(Some(var.varattno as i32))
}

/* ===========================================================================
 * dependency_is_compatible_expression (dependencies.c:1167)
 *
 * Like dependency_is_compatible_clause, but the operand need not be a simple
 * Var: on success it returns the matching statistics expression (a node from
 * one of the rel's statistics objects' `exprs`). `clause` is RestrictInfo-
 * unwrapped by the caller (we receive the bare clause Expr; the RestrictInfo
 * pseudoconstant/singleton checks are applied in the driver's per-clause loop).
 * ======================================================================== */

/// `dependency_is_compatible_expression(clause, relid, statlist, &expr)`
/// (dependencies.c:1167). Returns the index into `stat_exprs` (a flat list of
/// all dependency-stat expressions, paired with their owning order) when the
/// operand exactly matches a statistics expression. `stat_exprs` is the
/// concatenation of every dependency-kind statistics object's `exprs` (as
/// `&Expr`), in statlist order; a match returns the matching expression's
/// position so the caller can dedup with `equal`.
fn dependency_is_compatible_expression(
    clause: &Expr,
    relid: i32,
    stat_exprs: &[Expr],
    run: &PlannerRun<'_>,
) -> PgResult<Option<Expr>> {
    let clause_expr: &Expr = match clause {
        Expr::OpExpr(expr) | Expr::DistinctExpr(expr) | Expr::NullIfExpr(expr) => {
            if expr.args.len() != 2 {
                return Ok(None);
            }
            let cexpr = if sel_seam::is_pseudo_constant_clause::call(&expr.args[1])? {
                &expr.args[0]
            } else if sel_seam::is_pseudo_constant_clause::call(&expr.args[0])? {
                &expr.args[1]
            } else {
                return Ok(None);
            };
            if lsyscache::get_oprrest::call(expr.opno)? != F_EQSEL {
                return Ok(None);
            }
            cexpr
        }
        Expr::ScalarArrayOpExpr(expr) => {
            if !expr.useOr {
                return Ok(None);
            }
            if expr.args.len() != 2 {
                return Ok(None);
            }
            if !sel_seam::is_pseudo_constant_clause::call(&expr.args[1])? {
                return Ok(None);
            }
            let cexpr = &expr.args[0];
            if lsyscache::get_oprrest::call(expr.opno)? != F_EQSEL {
                return Ok(None);
            }
            cexpr
        }
        Expr::BoolExpr(b) if b.boolop == OR_EXPR => {
            // OR: all arguments must match the same statistics expression.
            let mut matched: Option<Expr> = None;
            for arg in &b.args {
                match dependency_is_compatible_expression(arg, relid, stat_exprs, run)? {
                    None => return Ok(None),
                    Some(or_expr) => {
                        match &matched {
                            None => matched = Some(or_expr),
                            Some(prev) => {
                                if !nodefuncs::equal::call(&or_expr, prev) {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }
            return Ok(matched);
        }
        Expr::BoolExpr(b) if b.boolop == NOT_EXPR => {
            // "NOT x" == "x = false".
            nodefuncs::get_notclausearg::call(clause)
        }
        _ => clause,
    };

    // Ignore any RelabelType above the operand.
    let clause_expr = strip_relabel(clause_expr);

    // Search for a matching statistics expression.
    for stat_expr in stat_exprs {
        if nodefuncs::equal::call(clause_expr, stat_expr) {
            return Ok(Some(stat_expr.clone_in(run.mcx())?));
        }
    }

    Ok(None)
}

/* ===========================================================================
 * dependencies_clauselist_selectivity (dependencies.c:1369)
 * ======================================================================== */

/// `dependencies_clauselist_selectivity(...)` (dependencies.c:1369). The
/// expression leg (negative pseudo-attnums) is faithfully retained but is empty
/// in practice while the build-side `stxexprs` expression-statistics leg is
/// deferred (no statistics object carries expressions), so `unique_exprs_cnt`
/// stays 0 (no attnum offset).
#[allow(clippy::too_many_arguments)]
fn dependencies_clauselist_selectivity(
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    rel: RelId,
    estimatedclauses: &mut Relids,
) -> PgResult<f64> {
    let mut s1: f64 = 1.0;

    if !has_stats_of_kind(root, rel, STATS_EXT_DEPENDENCIES) {
        return Ok(1.0);
    }

    let rel_relid = root.rel(rel).relid;
    let rte_inh = planner_rt_fetch(run, root, rel_relid).inh;

    // The concatenation of every dependency-stat object's expressions, used by
    // dependency_is_compatible_expression. With the stxexprs build leg deferred,
    // these lists are empty, so the expression path produces no unique_exprs.
    let dep_stat_exprs = collect_dependency_stat_exprs(root, rel, run)?;

    let nclauses = clauses.len();
    let mut list_attnums: Vec<i32> = Vec::with_capacity(nclauses);
    // Expressions get negative attnums (-1, -2, ...) deduped via equal().
    let mut unique_exprs: Vec<Expr> = Vec::new();

    for (listidx, &rid) in clauses.iter().enumerate() {
        let mut attnum = INVALID_ATTNUM;

        if !bms::relids_is_member::call(listidx as i32, estimatedclauses) {
            let rinfo = root.rinfo(rid);
            let pseudoconstant = rinfo.pseudoconstant;
            let singleton_ok =
                bms::relids_membership::call(&rinfo.clause_relids) == BMS_SINGLETON;
            let clause_node = rinfo.clause;

            if !pseudoconstant && singleton_ok {
                let clause: Expr = root.node(clause_node).clone_in(run.mcx())?;
                if let Some(a) = dependency_is_compatible_clause(&clause, rel_relid as i32)? {
                    // simple column reference
                    attnum = a;
                } else if let Some(expr) = dependency_is_compatible_expression(
                    &clause,
                    rel_relid as i32,
                    &dep_stat_exprs,
                    run,
                )? {
                    // expression: assign a negative attnum, deduping by equal().
                    let mut found = INVALID_ATTNUM;
                    for (i, ue) in unique_exprs.iter().enumerate() {
                        if nodefuncs::equal::call(ue, &expr) {
                            found = -((i as i32) + 1);
                            break;
                        }
                    }
                    if found == INVALID_ATTNUM {
                        unique_exprs.push(expr);
                        found = -(unique_exprs.len() as i32);
                    }
                    attnum = found;
                }
            }
        }

        list_attnums.push(attnum);
    }

    let unique_exprs_cnt = unique_exprs.len() as i32;

    // Offset enough for the lowest value (-unique_exprs_cnt) to become 1.
    let attnum_offset = if unique_exprs_cnt > 0 {
        unique_exprs_cnt + 1
    } else {
        0
    };

    let mut clauses_attnums: Relids = None;
    for i in 0..nclauses {
        if list_attnums[i] == INVALID_ATTNUM {
            continue;
        }
        let attnum = list_attnums[i] + attnum_offset;
        list_attnums[i] = attnum;
        clauses_attnums = bms::relids_add_member::call(clauses_attnums, attnum);
    }

    if bms::relids_membership::call(&clauses_attnums) != BMS_MULTIPLE {
        return Ok(1.0);
    }

    // Load functional dependencies for stats matching >= 2 attributes, remapping
    // each dependency's attnums (regular attrs offset; expressions translated to
    // the unique-expr attnum) and dropping dependencies not fully covered.
    let stat_oids = collect_stat_oids(
        root,
        rel,
        STATS_EXT_DEPENDENCIES,
        rte_inh,
        &clauses_attnums,
        attnum_offset,
        &unique_exprs,
        run,
    )?;

    let mut func_dependencies: Vec<MVDependencies> = Vec::new();
    for (stat_oid, stat_exprs) in stat_oids {
        let mut d = statext_dependencies_load(run.mcx(), stat_oid, rte_inh)?;

        if unique_exprs_cnt > 0 || !stat_exprs.is_empty() {
            remap_dependencies(
                &mut d,
                attnum_offset,
                &clauses_attnums,
                &stat_exprs,
                &unique_exprs,
            )?;
        }

        // It's possible we've removed all dependencies, in which case we don't
        // bother adding it to the list.
        if d.ndeps > 0 {
            func_dependencies.push(d);
        }
    }

    if func_dependencies.is_empty() {
        return Ok(1.0);
    }

    // Greedily pick the widest/strongest dependencies, removing each chosen
    // dependency's implied attribute from the working attnum set.
    let mut chosen: Vec<MVDependency> = Vec::new();
    loop {
        let attnums_vec = relids_to_vec(&clauses_attnums);
        match deps::find_strongest_dependency(&func_dependencies, &attnums_vec) {
            None => break,
            Some((i, j)) => {
                let dependency = (*func_dependencies[i].deps[j]).clone();
                let implied = dependency.attributes[dependency.nattributes as usize - 1];
                clauses_attnums = bms_del_member(clauses_attnums, implied as i32);
                chosen.push(dependency);
            }
        }
    }

    if !chosen.is_empty() {
        s1 = clauselist_apply_dependencies(
            run,
            root,
            clauses,
            var_relid,
            jointype,
            sjinfo,
            &chosen,
            &list_attnums,
            estimatedclauses,
        )?;
    }

    Ok(s1)
}

/* ===========================================================================
 * clauselist_apply_dependencies (dependencies.c:1013)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn clauselist_apply_dependencies(
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    dependencies: &[MVDependency],
    list_attnums: &[i32],
    estimatedclauses: &mut Relids,
) -> PgResult<f64> {
    // Extract the attnums of all implying and implied attributes.
    let mut attnums: Relids = None;
    for dependency in dependencies {
        for j in 0..dependency.nattributes as usize {
            attnums = bms::relids_add_member::call(attnums, dependency.attributes[j] as i32);
        }
    }

    // attr_sel slots, in bms_next_member order.
    let attnum_order = relids_to_vec(&attnums);
    let nattrs = attnum_order.len();
    let mut attr_sel: Vec<f64> = Vec::with_capacity(nattrs);

    for &attnum in &attnum_order {
        let mut attr_clauses: Vec<RinfoId> = Vec::new();
        for (listidx, &rid) in clauses.iter().enumerate() {
            if list_attnums[listidx] == attnum {
                attr_clauses.push(rid);
                *estimatedclauses = bms::relids_add_member::call(estimatedclauses.take(), listidx as i32);
            }
        }

        // C calls clauselist_selectivity_ext(..., use_extended_stats=false) so
        // the per-attribute simple selectivity cannot recursively re-enter
        // extended statistics while feeding the dependency combination.
        let simple_sel = sel_seam::clauselist_selectivity_ext::call(
            run,
            root,
            &attr_clauses,
            var_relid,
            jointype,
            sjinfo,
            false,
        )?;
        attr_sel.push(simple_sel);
    }

    // Map each dependency's attributes to attr_sel member-index positions
    // (bms_member_index), implying-first / implied-last, plus its degree.
    let mut dep_member_indexes: Vec<Vec<usize>> = Vec::with_capacity(dependencies.len());
    let mut dep_degrees: Vec<f64> = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let mut members: Vec<usize> = Vec::with_capacity(dependency.nattributes as usize);
        for j in 0..dependency.nattributes as usize {
            let attnum = dependency.attributes[j] as i32;
            let idx = attnum_order
                .iter()
                .position(|&a| a == attnum)
                .expect("clauselist_apply_dependencies: dependency attr not in attnum set");
            members.push(idx);
        }
        dep_member_indexes.push(members);
        dep_degrees.push(dependency.degree);
    }

    Ok(deps::combine_dependency_selectivities(
        &mut attr_sel,
        &dep_member_indexes,
        &dep_degrees,
    ))
}

/* ===========================================================================
 * statext_clauselist_selectivity (extended_stats.c:1981) — the seam entry.
 * ======================================================================== */

/// `statext_clauselist_selectivity(...)` (extended_stats.c:1981). The seam folds
/// the C `*estimatedclauses` in/out parameter into the returned tuple.
#[allow(clippy::too_many_arguments)]
pub fn statext_clauselist_selectivity(
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    rel: RelId,
    estimatedclauses: &Relids,
    is_or: bool,
) -> PgResult<(f64, Relids)> {
    let mut estimated = estimatedclauses.clone();

    // MCV leg: deferred match engine contributes its neutral identity (see note).
    let mut sel: f64 = if is_or { 0.0 } else { 1.0 };

    // Functional dependencies only work for clauses connected by AND.
    if is_or {
        return Ok((sel, estimated));
    }

    sel *= dependencies_clauselist_selectivity(
        run,
        root,
        clauses,
        var_relid,
        jointype,
        sjinfo,
        rel,
        &mut estimated,
    )?;

    Ok((clamp_probability(sel), estimated))
}

/* ===========================================================================
 * statext_dependencies_load (dependencies.c:601) — pg_statistic_ext_data load.
 * ======================================================================== */

/// `statext_dependencies_load(mvoid, inh)` (dependencies.c:601) — fetch and
/// deserialize the stored functional dependencies for one statistics object.
/// Ported over the `table_open`/`genam` systable substrate this crate already
/// uses (rather than the SysCache, which is unported); the row is keyed on
/// `(stxoid, stxdinherit)` via `pg_statistic_ext_data_stxoid_inh_index`.
/// Faithful to C: errors (`elog(ERROR)`) when the `pg_statistic_ext_data` row
/// is missing ("cache lookup failed") or when the dependencies column is NULL
/// ("requested statistics kind ... is not yet built").
fn statext_dependencies_load(
    mcx: Mcx<'_>,
    mvoid: Oid,
    inh: bool,
) -> PgResult<MVDependencies> {
    let mut skey = [ScanKeyData::empty(), ScanKeyData::empty()];
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_statistic_ext_data_stxoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(mvoid),
    )?;
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_statistic_ext_data_stxdinherit,
        BTEqualStrategyNumber,
        types_core::fmgr::F_BOOLEQ,
        Datum::from_bool(inh),
    )?;

    let pg_stextdata = table_open(mcx, StatisticExtDataRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan::call(
        &pg_stextdata,
        StatisticExtDataStxoidInhIndexId,
        true,
        None,
        &skey,
    )?;

    let htup = genam::systable_getnext::call(mcx, scan.desc_mut())?;

    let result = match htup {
        None => {
            // C: elog(ERROR, "cache lookup failed for statistics object %u").
            drop(scan);
            pg_stextdata.close(AccessShareLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for statistics object {}",
                mvoid
            )));
        }
        Some(htup) => {
            let row = backend_access_common_heaptuple::heap_deform_tuple(
                mcx,
                &htup.tuple,
                &pg_stextdata.rd_att,
                &htup.data,
            )?;
            let (d, isnull) = &row[(Anum_pg_statistic_ext_data_stxddependencies - 1) as usize];
            if *isnull {
                // C: elog(ERROR, "requested statistics kind \"%c\" is not yet
                // built for statistics object %u", STATS_EXT_DEPENDENCIES, mvoid).
                drop(scan);
                pg_stextdata.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "requested statistics kind \"d\" is not yet built for statistics object {}",
                    mvoid
                )));
            }
            // statext_dependencies_deserialize wants the VARSIZE_ANY_EXHDR body.
            let body = varlena_body(d.as_ref_bytes())?;
            // DatumGetByteaPP(deps) is non-NULL here, so deserialize yields Some.
            deps::statext_dependencies_deserialize(mcx, Some(body))?
                .ok_or_else(|| PgError::error("statext_dependencies_load: deserialize returned NULL"))?
        }
    };

    drop(scan);
    pg_stextdata.close(AccessShareLock)?;

    Ok(result)
}

/// `VARDATA_ANY(ptr)` — the bytea payload past the (1-byte short or 4-byte long)
/// varlena header.
fn varlena_body(data: &[u8]) -> PgResult<&[u8]> {
    if data.is_empty() {
        return Err(PgError::error("statext_dependencies_load: empty bytea"));
    }
    // VARATT_IS_1B: low bit of the first header byte set.
    if (data[0] & 0x01) == 0x01 {
        Ok(&data[1..])
    } else {
        if data.len() < 4 {
            return Err(PgError::error(
                "statext_dependencies_load: truncated bytea header",
            ));
        }
        Ok(&data[4..])
    }
}

/* ===========================================================================
 * helpers over the planner arena / Relids
 * ======================================================================== */

/// `has_stats_of_kind(rel->statlist, requiredkind)` (extended_stats.c).
fn has_stats_of_kind(root: &PlannerInfo, rel: RelId, requiredkind: i8) -> bool {
    root.rel(rel)
        .statlist
        .iter()
        .any(|&id| root.statistic_ext(id).kind == requiredkind)
}

/// The concatenation of every dependency-kind statistics object's expressions
/// on `rel` (as owned `Expr`s), for `dependency_is_compatible_expression`. With
/// the stxexprs build leg deferred these lists are empty.
fn collect_dependency_stat_exprs(
    root: &PlannerInfo,
    rel: RelId,
    run: &PlannerRun<'_>,
) -> PgResult<Vec<Expr>> {
    let mut out = Vec::new();
    let statlist = root.rel(rel).statlist.clone();
    for id in statlist {
        let stat = root.statistic_ext(id);
        if stat.kind != STATS_EXT_DEPENDENCIES {
            continue;
        }
        let expr_ids = stat.exprs.clone();
        for eid in expr_ids {
            out.push(root.node(eid).clone_in(run.mcx())?);
        }
    }
    Ok(out)
}

/// Collect the `(statOid, exprs)` of dependency-kind statistics objects on `rel`
/// whose inheritance flag matches `rte_inh` and which match at least two clause
/// attnums or expressions (the C "skip objects matching fewer than two
/// attributes/expressions" gate). `exprs` is the object's own expression list,
/// passed on to the per-dependency remapping.
fn collect_stat_oids(
    root: &PlannerInfo,
    rel: RelId,
    requiredkind: i8,
    rte_inh: bool,
    clauses_attnums: &Relids,
    attnum_offset: i32,
    unique_exprs: &[Expr],
    run: &PlannerRun<'_>,
) -> PgResult<Vec<(Oid, Vec<Expr>)>> {
    let mut out = Vec::new();
    let statlist = root.rel(rel).statlist.clone();
    for id in statlist {
        let stat = root.statistic_ext(id);
        if stat.kind != requiredkind {
            continue;
        }
        if stat.inherit != rte_inh {
            continue;
        }

        // Count matching attributes (offset to match clauses_attnums); skip
        // expression keys (non-user-defined attnums).
        let mut nmatched = 0;
        for k in relids_to_vec(&stat.keys) {
            if !attr_is_user_defined(k) {
                continue;
            }
            let attnum = k + attnum_offset;
            if bms::relids_is_member::call(attnum, clauses_attnums) {
                nmatched += 1;
            }
        }

        // Resolve and count matching expressions.
        let stat_exprs: Vec<Expr> = {
            let expr_ids = stat.exprs.clone();
            let mut v = Vec::with_capacity(expr_ids.len());
            for eid in expr_ids {
                v.push(root.node(eid).clone_in(run.mcx())?);
            }
            v
        };
        let mut nexprs = 0;
        for ue in unique_exprs {
            for stat_expr in &stat_exprs {
                if nodefuncs::equal::call(stat_expr, ue) {
                    nexprs += 1;
                }
            }
        }

        if nmatched + nexprs < 2 {
            continue;
        }

        out.push((stat.stat_oid, stat_exprs));
    }
    Ok(out)
}

/// The per-dependency attnum remapping (dependencies.c:1657-1758): for each
/// dependency, offset its regular attnums and translate its expression attnums
/// to the unique-expr attnum; drop dependencies that reference an attribute or
/// expression not present in the clauses. Mutates `deps` in place (compacting
/// the kept dependencies and updating `ndeps`).
fn remap_dependencies(
    deps: &mut MVDependencies,
    attnum_offset: i32,
    clauses_attnums: &Relids,
    stat_exprs: &[Expr],
    unique_exprs: &[Expr],
) -> PgResult<()> {
    let mut ndeps = 0usize;
    let total = deps.ndeps as usize;
    for i in 0..total {
        let mut skip = false;
        // Walk the dependency's attributes, remapping in place.
        for j in 0..deps.deps[i].nattributes as usize {
            let attnum = deps.deps[i].attributes[j] as i32;

            if attr_is_user_defined(attnum) {
                // Regular attribute: offset and check membership.
                let mapped = attnum + attnum_offset;
                deps.deps[i].attributes[j] = mapped as i16;
                if !bms::relids_is_member::call(mapped, clauses_attnums) {
                    skip = true;
                    break;
                }
                continue;
            }

            // Expression: translate the negative attnum to the stat-expr index,
            // then to the unique-expr attnum.
            let idx = (-(1 + attnum)) as usize;
            if idx >= stat_exprs.len() {
                // C asserts this is in range; defensively skip if not.
                skip = true;
                break;
            }
            let expr = &stat_exprs[idx];

            let mut unique_attnum = INVALID_ATTNUM;
            for (m, ue) in unique_exprs.iter().enumerate() {
                if nodefuncs::equal::call(ue, expr) {
                    unique_attnum = -((m as i32) + 1) + attnum_offset;
                    break;
                }
            }

            if unique_attnum == INVALID_ATTNUM {
                // No matching expression: the dependency can't be fully covered.
                skip = true;
                break;
            }

            deps.deps[i].attributes[j] = unique_attnum as i16;
        }

        if !skip {
            if ndeps != i {
                deps.deps.swap(ndeps, i);
            }
            ndeps += 1;
        }
    }

    deps.deps.truncate(ndeps);
    deps.ndeps = ndeps as u32;
    Ok(())
}

/// Materialize a `Relids` as an ascending `Vec<i32>` (the `bms_next_member` walk).
fn relids_to_vec(relids: &Relids) -> Vec<i32> {
    let mut out = Vec::new();
    let mut prev = -1;
    loop {
        let next = bms::relids_next_member::call(relids, prev);
        if next < 0 {
            break;
        }
        out.push(next);
        prev = next;
    }
    out
}

/// `bms_del_member(a, x)` (bitmapset.c) — clear bit `x` from the set. No relnode
/// seam exists for delete; the operation is unambiguous bit-clearing over the
/// public word storage (`Bitmapset.words`), matching the C exactly.
fn bms_del_member(a: Relids, x: i32) -> Relids {
    // bms_del_member(NULL, x) is a no-op returning NULL; x < 0 is invalid in C.
    let mut set = match a {
        None => return None,
        Some(b) => b,
    };
    if x < 0 {
        return Some(set);
    }
    const BITS_PER_WORD: i32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = (x % BITS_PER_WORD) as u32;
    if wordnum < set.words.len() {
        set.words[wordnum] &= !(1u64 << bitnum);
    }
    // If the set became empty, normalize to None (bms_del_member returns NULL).
    if set.words.iter().all(|&w| w == 0) {
        return None;
    }
    Some(set)
}

// Keep `Bitmapset` referenced (the del-member math uses its public `words`).
#[allow(dead_code)]
fn _assert_bitmapset(_: &Bitmapset) {}
