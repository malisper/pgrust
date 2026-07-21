//! Planner consumption of extended statistics (extended_stats.c's
//! statext_clauselist_selectivity + dependencies.c/mcv.c selectivity legs +
//! plancat.c's get_relation_statistics), including expression statistics.

use mcx::PgVec;
// Scratch on the stats-present path uses std Vec where element types carry
// lifetimes awkwardly; per-query, bounded by clause/statistics counts.
use types_error::PgResult;
use types_nodes::Node;
use types_pathnodes::{
    JoinType, RelId, Relids, RinfoId, SpecialJoinInfo, StatisticExtInfo,
};

use crate::relnode::{relids_is_member, relids_is_subset, relids_num_members};
use crate::run::PlannerRun;

const STATS_EXT_NDISTINCT: i8 = b'd' as i8;
const STATS_EXT_DEPENDENCIES: i8 = b'f' as i8;
const STATS_EXT_MCV: i8 = b'm' as i8;
const STATS_EXT_EXPRESSIONS: i8 = b'e' as i8;
const STATS_MAX_DIMENSIONS: usize = 8;

const F_EQSEL: u32 = 101;
const F_NEQSEL: u32 = 102;
const F_SCALARLTSEL: u32 = 103;
const F_SCALARGTSEL: u32 = 104;
const F_SCALARLESEL: u32 = 336;
const F_SCALARGESEL: u32 = 337;

const Anum_data_stxdndistinct: i32 = 3;
const Anum_data_stxddependencies: i32 = 4;
const Anum_data_stxdmcv: i32 = 5;

fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

fn attnums_from_members<'mcx>(run: &PlannerRun<'mcx>, members: &[i16]) -> Relids<'mcx> {
    let mut r: Relids<'mcx> = crate::relnode::relids_empty();
    for &m in members {
        r = crate::relnode::relids_union(
            run.mcx,
            &r,
            &crate::relnode::relids_singleton(run.mcx, m as u32),
        );
    }
    r
}

// get_relation_statistics (plancat.c).
pub fn get_relation_statistics<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    relid: types_core::Oid,
) -> PgResult<()> {
    let mcx = run.mcx;
    let statoids = relcache_seams::relation_get_stat_ext_list::call(mcx, relid)?;
    let mut statlist: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let varno = run.root.rel(rel).relid as i32;
    for &statoid in statoids.iter() {
        let form = syscache_seams::statext_form::call(mcx, statoid)?
            .unwrap_or_else(|| panic!("cache lookup failed for statistics object {statoid}"));
        let keys = attnums_from_members(run, &form.keys);
        // eval_const_expressions + varno fixup so the trees compare equal()
        // to similarly-processed qual clauses (opfuncids match via the
        // either-zero rule, as with index expressions).
        let mut exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
        if form.has_exprs {
            let src = syscache_seams::statext_exprs_src::call(mcx, statoid)?
                .expect("stxexprs non-null");
            let node = readfuncs::stringToNode(mcx, src.as_str())?;
            let list = node.as_list().expect("stxexprs is a List");
            for e in list.iter() {
                let e = clauses::eval_const_expressions(mcx, e)?;
                if varno != 1 {
                    crate::plancat::change_var_nodes(e, varno)?;
                }
                exprs.push(run.intern_expr(e));
            }
        }
        for inh in [true, false] {
            let Some((nd, deps, mcv, exp)) =
                syscache_seams::statext_data_kinds::call(statoid, inh)?
            else {
                continue;
            };
            for (built, kind) in [
                (nd, STATS_EXT_NDISTINCT),
                (deps, STATS_EXT_DEPENDENCIES),
                (mcv, STATS_EXT_MCV),
                (exp, STATS_EXT_EXPRESSIONS),
            ] {
                if !built || !form.kinds.contains(&(kind as u8)) {
                    continue;
                }
                let mut info_exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
                info_exprs.extend(exprs.iter().copied());
                let info = StatisticExtInfo {
                    stat_oid: statoid,
                    inherit: inh,
                    rel: Some(rel),
                    kind,
                    keys: clone_relids(run, &keys),
                    exprs: info_exprs,
                };
                statlist.push(run.root.alloc_statistic_ext(info));
            }
        }
    }
    run.root.rel_mut(rel).statlist = statlist;
    Ok(())
}

fn clone_relids<'mcx>(run: &PlannerRun<'mcx>, r: &Relids<'mcx>) -> Relids<'mcx> {
    crate::relnode::relids_copy(run.mcx, r)
}

fn has_stats_of_kind(run: &PlannerRun<'_>, rel: RelId, requiredkind: i8) -> bool {
    run.root
        .rel(rel)
        .statlist
        .iter()
        .any(|&id| run.root.statistic_ext(id).kind == requiredkind)
}

fn stat_exprs<'mcx>(run: &PlannerRun<'mcx>, id: types_pathnodes::NodeId) -> Vec<Node<'mcx>> {
    run.root
        .statistic_ext(id)
        .exprs
        .iter()
        .map(|&eid| *run.root.expr_node(eid))
        .collect()
}

// find_single_rel_for_clauses (clausesel.c). Every input is a RestrictInfo,
// so C's bare-AND-clause and non-RestrictInfo arms have no analog here.
pub fn find_single_rel_for_clauses<'mcx>(
    run: &PlannerRun<'mcx>,
    clauses: &[RinfoId],
) -> Option<RelId> {
    let mut lastrelid: i32 = 0;
    for &rid in clauses {
        let r = run.root.rinfo(rid);
        if crate::relnode::relids_is_empty(&r.clause_relids) {
            continue;
        }
        let Some(relid) = crate::relnode::relids_singleton_member(&r.clause_relids) else {
            return None;
        };
        if lastrelid == 0 {
            lastrelid = relid;
        } else if relid != lastrelid {
            return None;
        }
    }
    if lastrelid != 0 {
        return run.root.simple_rel_array.get(lastrelid as usize).copied().flatten();
    }
    None
}

// find_single_rel_for_clauses (clausesel.c), bare-node form: C's OR args are
// sub-RestrictInfos carrying clause_relids; the port pulls varnos instead.
pub fn find_single_rel_for_clause_nodes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
) -> PgResult<Option<RelId>> {
    let mut lastrelid: i32 = 0;
    for &clause in clauses {
        let bms = vars::pull_varnos(run.mcx, clause)?;
        let mut relid: i32 = 0;
        for r in bms.iter() {
            if relids_is_member(r, &run.root.outer_join_rels) {
                continue;
            }
            if relid == 0 {
                relid = r;
            } else if r != relid {
                return Ok(None);
            }
        }
        if relid == 0 {
            continue;
        }
        if lastrelid == 0 {
            lastrelid = relid;
        } else if relid != lastrelid {
            return Ok(None);
        }
    }
    if lastrelid != 0 {
        return Ok(run.root.simple_rel_array.get(lastrelid as usize).copied().flatten());
    }
    Ok(None)
}

// statext_clauselist_selectivity (extended_stats.c), AND-list leg (the OR
// leg is statext_clauselist_selectivity_or_nodes below).
pub fn statext_clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    rel: RelId,
    estimated: &mut [bool],
) -> PgResult<f64> {
    let mut sel =
        statext_mcv_clauselist_selectivity(run, clauses, varrelid, jointype, sjinfo, rel, estimated)?;
    sel *= dependencies_clauselist_selectivity(run, clauses, varrelid, jointype, sjinfo, rel, estimated)?;
    Ok(sel)
}

// statext_clauselist_selectivity (extended_stats.c), is_or=true leg: MCV
// only — functional dependencies apply to ANDed lists only.
pub fn statext_clauselist_selectivity_or_nodes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    rel: RelId,
    estimated: &mut [bool],
) -> PgResult<f64> {
    statext_mcv_clauselist_selectivity_or_nodes(
        run, clauses, varrelid, jointype, sjinfo, rel, estimated,
    )
}

// statext_is_compatible_clause_internal (bare node): collects referenced
// attnums and primitive sub-expressions to be matched against statistics.
fn compatible_internal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
    attnums: &mut Relids<'mcx>,
    exprs: &mut Vec<Node<'mcx>>,
    leakproof: &mut bool,
) -> PgResult<bool> {
    let clause = strip_relabel(clause);

    if let Some(var) = clause.as_var() {
        if var.varno != relid || var.varlevelsup != 0 || var.varattno <= 0 {
            return Ok(false);
        }
        let single = crate::relnode::relids_singleton(run.mcx, var.varattno as u32);
        *attnums = crate::relnode::relids_union(run.mcx, attnums, &single);
        return Ok(true);
    }

    if let Some(op) = clause.as_op_expr() {
        if op.args.len() != 2 {
            return Ok(false);
        }
        let Some((expr, _cst, _onleft)) = examine_opclause_args(op.args.nth(0), op.args.nth(1))
        else {
            return Ok(false);
        };
        match lsyscache::get_oprrest(op.opno)? {
            F_EQSEL | F_NEQSEL | F_SCALARLTSEL | F_SCALARLESEL | F_SCALARGTSEL
            | F_SCALARGESEL => {}
            _ => return Ok(false),
        }
        if *leakproof {
            *leakproof = lsyscache::get_func_leakproof(lsyscache::get_opcode(op.opno)?)?;
        }
        if expr.as_var().is_some() {
            return compatible_internal(run, expr, relid, attnums, exprs, leakproof);
        }
        exprs.push(expr);
        return Ok(true);
    }

    if let Some(saop) = clause.as_scalar_array_op_expr() {
        if saop.args.len() != 2 {
            return Ok(false);
        }
        let Some((expr, _cst, expronleft)) = examine_opclause_args(saop.args.nth(0), saop.args.nth(1))
        else {
            return Ok(false);
        };
        if !expronleft {
            return Ok(false);
        }
        match lsyscache::get_oprrest(saop.opno)? {
            F_EQSEL | F_NEQSEL | F_SCALARLTSEL | F_SCALARLESEL | F_SCALARGTSEL
            | F_SCALARGESEL => {}
            _ => return Ok(false),
        }
        if *leakproof {
            *leakproof = lsyscache::get_func_leakproof(lsyscache::get_opcode(saop.opno)?)?;
        }
        if expr.as_var().is_some() {
            return compatible_internal(run, expr, relid, attnums, exprs, leakproof);
        }
        exprs.push(expr);
        return Ok(true);
    }

    if let Some(b) = clause.as_bool_expr() {
        for arg in &b.args {
            if !compatible_internal(run, arg, relid, attnums, exprs, leakproof)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    if let Some(nt) = clause.as_null_test() {
        let arg = nt.arg.expect("NullTest arg");
        if arg.as_var().is_some() {
            return compatible_internal(run, arg, relid, attnums, exprs, leakproof);
        }
        exprs.push(arg);
        return Ok(true);
    }

    exprs.push(clause);
    Ok(true)
}

fn strip_relabel(clause: Node<'_>) -> Node<'_> {
    match clause.as_relabel_type() {
        Some(r) => r.arg,
        None => clause,
    }
}

fn examine_opclause_args<'mcx>(
    leftop: Node<'mcx>,
    rightop: Node<'mcx>,
) -> Option<(Node<'mcx>, &'mcx types_nodes::primnodes::Const, bool)> {
    let leftop = strip_relabel(leftop);
    let rightop = strip_relabel(rightop);
    if let Some(cst) = rightop.as_const() {
        Some((leftop, cst, true))
    } else {
        leftop.as_const().map(|cst| (rightop, cst, false))
    }
}

fn statext_is_compatible_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: RinfoId,
    relid: i32,
) -> PgResult<Option<(Relids<'mcx>, Vec<Node<'mcx>>)>> {
    {
        let r = run.root.rinfo(rid);
        if r.pseudoconstant {
            return Ok(None);
        }
        match crate::relnode::relids_singleton_member(&r.clause_relids) {
            Some(cr) if cr == relid => {}
            _ => return Ok(None),
        }
    }
    let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
    compatible_clause_tail(run, clause, relid)
}

// statext_is_compatible_clause (extended_stats.c), bare-node form: the
// sub-RestrictInfo relid check becomes a pull_varnos singleton probe.
fn statext_is_compatible_clause_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
) -> PgResult<Option<(Relids<'mcx>, Vec<Node<'mcx>>)>> {
    let bms = vars::pull_varnos(run.mcx, clause)?;
    let mut clause_relid: i32 = 0;
    for r in bms.iter() {
        if relids_is_member(r, &run.root.outer_join_rels) {
            continue;
        }
        if clause_relid == 0 {
            clause_relid = r;
        } else if r != clause_relid {
            return Ok(None);
        }
    }
    if clause_relid != relid {
        return Ok(None);
    }
    compatible_clause_tail(run, clause, relid)
}

fn compatible_clause_tail<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
) -> PgResult<Option<(Relids<'mcx>, Vec<Node<'mcx>>)>> {
    let mut attnums: Relids<'mcx> = crate::relnode::relids_empty();
    let mut exprs: Vec<Node<'mcx>> = Vec::new();
    let mut leakproof = true;
    if !compatible_internal(run, clause, relid, &mut attnums, &mut exprs, &mut leakproof)? {
        return Ok(None);
    }
    // Non-leakproof operators may reveal MCV values; require every row of
    // the referenced columns to be readable (Vars inside sub-expressions
    // included).
    if !leakproof {
        let mut cols: PgVec<'_, i16> = PgVec::new_in(run.mcx);
        cols.extend(crate::relnode::relids_members(&attnums).map(|a| a as i16));
        for &e in &exprs {
            let mut bm = types_nodes::Bitmapset::empty();
            vars::pull_varattnos(run.mcx, e, relid, &mut bm)?;
            for m in bm.iter() {
                let a = (m + types_tuple::htup::FirstLowInvalidHeapAttributeNumber) as i16;
                if !cols.contains(&a) {
                    cols.push(a);
                }
            }
        }
        if !crate::selfuncs::all_rows_selectable(run, &run.root, relid, Some(&cols))? {
            return Ok(None);
        }
    }
    Ok(Some((attnums, exprs)))
}

// stat_find_expression (extended_stats.c).
fn stat_find_expression(sexprs: &[Node<'_>], expr: Node<'_>) -> Option<usize> {
    sexprs.iter().position(|&se| types_nodes::equal(expr, se))
}

// stat_covers_expressions (extended_stats.c).
fn stat_covers_expressions(
    sexprs: &[Node<'_>],
    exprs: &[Node<'_>],
    expr_idxs: Option<&mut Vec<usize>>,
) -> bool {
    let mut idxs: Vec<usize> = Vec::new();
    for &e in exprs {
        let Some(i) = stat_find_expression(sexprs, e) else { return false };
        idxs.push(i);
    }
    if let Some(out) = expr_idxs {
        *out = idxs;
    }
    true
}

// choose_best_statistics (extended_stats.c).
fn choose_best_statistics(
    run: &PlannerRun<'_>,
    rel: RelId,
    requiredkind: i8,
    inh: bool,
    clause_data: &[Option<(Relids<'_>, Vec<Node<'_>>)>],
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut best_num_matched = 2;
    let mut best_match_keys = STATS_MAX_DIMENSIONS as i32 + 1;
    for (si, &id) in run.root.rel(rel).statlist.iter().enumerate() {
        {
            let info = run.root.statistic_ext(id);
            if info.kind != requiredkind || info.inherit != inh {
                continue;
            }
        }
        let sexprs = stat_exprs(run, id);
        let info = run.root.statistic_ext(id);
        let mut matched: Vec<i32> = Vec::new();
        let mut matched_exprs: Vec<usize> = Vec::new();
        for cd in clause_data.iter() {
            let Some((ca, ce)) = cd else { continue };
            if !relids_is_subset(ca, &info.keys) {
                continue;
            }
            let mut expr_idxs: Vec<usize> = Vec::new();
            if !stat_covers_expressions(&sexprs, ce, Some(&mut expr_idxs)) {
                continue;
            }
            for (i, w) in crate::relnode::relids_word_slice(ca).iter().enumerate() {
                let mut w = *w;
                while w != 0 {
                    let m = (i * 64) as i32 + w.trailing_zeros() as i32;
                    if !matched.contains(&m) {
                        matched.push(m);
                    }
                    w &= w - 1;
                }
            }
            for ei in expr_idxs {
                if !matched_exprs.contains(&ei) {
                    matched_exprs.push(ei);
                }
            }
        }
        let num_matched = (matched.len() + matched_exprs.len()) as i32;
        let numkeys = relids_num_members(&info.keys) + info.exprs.len() as i32;
        if num_matched > best_num_matched
            || (num_matched == best_num_matched && numkeys < best_match_keys)
        {
            best = Some(si);
            best_num_matched = num_matched;
            best_match_keys = numkeys;
        }
    }
    best
}

fn load_mcv<'mcx>(
    run: &PlannerRun<'mcx>,
    statoid: types_core::Oid,
    inh: bool,
) -> PgResult<statistics::mcv::MCVList<'mcx>> {
    let img = syscache_seams::statext_data_blob::call(run.mcx, statoid, inh, Anum_data_stxdmcv)?
        .unwrap_or_else(|| {
            panic!("requested statistics kind \"m\" is not yet built for statistics object {statoid}")
        });
    statistics::mcv::statext_mcv_deserialize(run.mcx, &img[4..])
}

// statext_mcv_clauselist_selectivity (extended_stats.c), AND form.
#[allow(clippy::too_many_arguments)]
fn statext_mcv_clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    rel: RelId,
    estimated: &mut [bool],
) -> PgResult<f64> {
    let mut sel = 1.0f64;
    if !has_stats_of_kind(run, rel, STATS_EXT_MCV) {
        return Ok(sel);
    }
    let relid = run.root.rel(rel).relid as i32;
    let inh = run.rte(relid as usize).inh;

    let mut clause_data: Vec<Option<(Relids<'mcx>, Vec<Node<'mcx>>)>> =
        Vec::with_capacity(clauses.len());
    for (i, &rid) in clauses.iter().enumerate() {
        if estimated[i] {
            clause_data.push(None);
        } else {
            clause_data.push(statext_is_compatible_clause(run, rid, relid)?);
        }
    }

    loop {
        let Some(si) = choose_best_statistics(run, rel, STATS_EXT_MCV, inh, &clause_data)
        else {
            break;
        };
        let stat_id = run.root.rel(rel).statlist[si];
        let sexprs = stat_exprs(run, stat_id);
        let (stat_oid, stat_keys) = {
            let info = run.root.statistic_ext(stat_id);
            (info.stat_oid, clone_relids(run, &info.keys))
        };

        let mut stat_clauses: Vec<RinfoId> = Vec::new();
        for (i, &rid) in clauses.iter().enumerate() {
            let Some((ca, ce)) = &clause_data[i] else { continue };
            if !relids_is_subset(ca, &stat_keys)
                || !stat_covers_expressions(&sexprs, ce, None)
            {
                continue;
            }
            stat_clauses.push(rid);
            estimated[i] = true;
            clause_data[i] = None;
        }

        let simple_sel = crate::clausesel::clauselist_selectivity_ext(
            run,
            &stat_clauses,
            varrelid,
            jointype,
            sjinfo,
            false,
        )?;
        let (mcv_sel, mcv_basesel, mcv_totalsel) =
            mcv_clauselist_selectivity(run, stat_oid, inh, &stat_keys, &sexprs, &stat_clauses)?;
        let stat_sel = mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel);
        sel *= stat_sel;
    }
    Ok(sel)
}

// statext_mcv_clauselist_selectivity (extended_stats.c), is_or=true form,
// with mcv_clause_selectivity_or (mcv.c:2127) inlined over the shared
// or_matches bitmap.
fn statext_mcv_clauselist_selectivity_or_nodes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    rel: RelId,
    estimated: &mut [bool],
) -> PgResult<f64> {
    let mut sel = 0.0f64;
    if !has_stats_of_kind(run, rel, STATS_EXT_MCV) {
        return Ok(sel);
    }
    let relid = run.root.rel(rel).relid as i32;
    let inh = run.rte(relid as usize).inh;

    let mut clause_data: Vec<Option<(Relids<'mcx>, Vec<Node<'mcx>>)>> =
        Vec::with_capacity(clauses.len());
    for (i, &clause) in clauses.iter().enumerate() {
        if estimated[i] {
            clause_data.push(None);
        } else {
            clause_data.push(statext_is_compatible_clause_node(run, clause, relid)?);
        }
    }

    loop {
        let Some(si) = choose_best_statistics(run, rel, STATS_EXT_MCV, inh, &clause_data)
        else {
            break;
        };
        let stat_id = run.root.rel(rel).statlist[si];
        let sexprs = stat_exprs(run, stat_id);
        let (stat_oid, stat_keys) = {
            let info = run.root.statistic_ext(stat_id);
            (info.stat_oid, clone_relids(run, &info.keys))
        };

        let mut stat_clauses: Vec<Node<'mcx>> = Vec::new();
        let mut simple_clauses: Vec<bool> = Vec::new();
        for (i, &clause) in clauses.iter().enumerate() {
            let Some((ca, ce)) = &clause_data[i] else { continue };
            if !relids_is_subset(ca, &stat_keys)
                || !stat_covers_expressions(&sexprs, ce, None)
            {
                continue;
            }
            simple_clauses.push(
                (crate::relnode::relids_is_unset(ca) && ce.len() == 1)
                    || (ce.is_empty() && relids_num_members(ca) == 1),
            );
            stat_clauses.push(clause);
            estimated[i] = true;
            clause_data[i] = None;
        }

        let mcv = load_mcv(run, stat_oid, inh)?;
        let mut or_matches: Vec<bool> = vec![false; mcv.items.len()];
        let mut simple_or_sel = 0.0f64;
        let mut stat_sel = 0.0f64;
        for (listidx, &clause) in stat_clauses.iter().enumerate() {
            let simple_sel = crate::clausesel::clause_selectivity_node_ext(
                run, clause, varrelid, jointype, sjinfo, false,
            )?;
            let overlap_simple_sel = simple_or_sel * simple_sel;
            simple_or_sel = clamp_probability(simple_or_sel + simple_sel - overlap_simple_sel);

            let new_matches =
                mcv_get_match_bitmap(run, &[clause], &stat_keys, &sexprs, &mcv, false)?;
            let mut mcv_sel = 0.0;
            let mut mcv_basesel = 0.0;
            let mut overlap_mcvsel = 0.0;
            let mut overlap_basesel = 0.0;
            let mut mcv_totalsel = 0.0;
            for (i, item) in mcv.items.iter().enumerate() {
                mcv_totalsel += item.frequency;
                if new_matches[i] {
                    mcv_sel += item.frequency;
                    mcv_basesel += item.base_frequency;
                    if or_matches[i] {
                        overlap_mcvsel += item.frequency;
                        overlap_basesel += item.base_frequency;
                    }
                }
                or_matches[i] = or_matches[i] || new_matches[i];
            }

            let clause_sel = if simple_clauses[listidx] {
                simple_sel
            } else {
                mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel)
            };
            let overlap_sel = mcv_combine_selectivities(
                overlap_simple_sel,
                overlap_mcvsel,
                overlap_basesel,
                mcv_totalsel,
            );
            stat_sel = clamp_probability(stat_sel + clause_sel - overlap_sel);
        }
        sel = sel + stat_sel - sel * stat_sel;
    }
    Ok(sel)
}

pub fn mcv_combine_selectivities(
    simple_sel: f64,
    mcv_sel: f64,
    mcv_basesel: f64,
    mcv_totalsel: f64,
) -> f64 {
    let mut other_sel = clamp_probability(simple_sel - mcv_basesel);
    if other_sel > 1.0 - mcv_totalsel {
        other_sel = 1.0 - mcv_totalsel;
    }
    clamp_probability(mcv_sel + other_sel)
}

fn mcv_clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    stat_oid: types_core::Oid,
    inh: bool,
    keys: &Relids<'mcx>,
    sexprs: &[Node<'mcx>],
    clauses: &[RinfoId],
) -> PgResult<(f64, f64, f64)> {
    let mcv = load_mcv(run, stat_oid, inh)?;
    let nodes: Vec<Node<'mcx>> = clauses
        .iter()
        .map(|&rid| *run.root.expr_node(run.root.rinfo(rid).clause))
        .collect();
    let matches = mcv_get_match_bitmap(run, &nodes, keys, sexprs, &mcv, false)?;
    let mut s = 0.0;
    let mut basesel = 0.0;
    let mut totalsel = 0.0;
    for (i, item) in mcv.items.iter().enumerate() {
        totalsel += item.frequency;
        if matches[i] {
            basesel += item.base_frequency;
            s += item.frequency;
        }
    }
    Ok((s, basesel, totalsel))
}

fn bms_member_index(keys: &Relids<'_>, attnum: i16) -> usize {
    // Unset (not merely all-zero) keys are the caller bug this guards.
    assert!(!crate::relnode::relids_is_unset(keys), "mcv_match_expression: empty keys");
    let mut idx = 0usize;
    for (i, w) in crate::relnode::relids_word_slice(keys).iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            let m = (i * 64) as i32 + w.trailing_zeros() as i32;
            if m == attnum as i32 {
                return idx;
            }
            idx += 1;
            w &= w - 1;
        }
    }
    panic!("variable not found in statistics object")
}

// mcv_match_expression (mcv.c): zero-based statistics dimension of the
// attribute or expression; expressions are stored after the simple columns.
fn mcv_match_expression(
    expr: Node<'_>,
    keys: &Relids<'_>,
    sexprs: &[Node<'_>],
) -> (usize, types_core::Oid) {
    if let Some(var) = expr.as_var() {
        return (bms_member_index(keys, var.varattno), var.varcollid);
    }
    let base = relids_num_members(keys) as usize;
    let idx = sexprs
        .iter()
        .position(|&se| types_nodes::equal(expr, se))
        .unwrap_or_else(|| panic!("expression not found in statistics object"));
    (base + idx, crate::pathkeys::expr_collation(expr))
}

// mcv_get_match_bitmap (mcv.c).
fn mcv_get_match_bitmap<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    keys: &Relids<'mcx>,
    sexprs: &[Node<'mcx>],
    mcvlist: &statistics::mcv::MCVList<'_>,
    is_or: bool,
) -> PgResult<Vec<bool>> {
    let mut matches: Vec<bool> = vec![!is_or; mcvlist.items.len()];

    for &clause in clauses {
        if let Some(op) = clause.as_op_expr().filter(|o| o.args.len() == 2) {
            let Some((clause_expr, cst, expronleft)) =
                examine_opclause_args(op.args.nth(0), op.args.nth(1))
            else {
                panic!("incompatible clause")
            };
            let (idx, collid) = mcv_match_expression(clause_expr, keys, sexprs);
            let opcode = lsyscache::get_opcode(op.opno)?;
            let mut opproc = fmgr_seams::fmgr_info::call(opcode)?;
            for (i, item) in mcvlist.items.iter().enumerate() {
                if item.isnull[idx] || cst.constisnull {
                    matches[i] = result_merge(matches[i], is_or, false);
                    continue;
                }
                if result_is_final(matches[i], is_or) {
                    continue;
                }
                let m = if expronleft {
                    types_fmgr::function_call2_coll_in(
                        &mut opproc,
                        collid,
                        run.mcx,
                        item.values[idx],
                        cst.constvalue,
                    )?
                } else {
                    types_fmgr::function_call2_coll_in(
                        &mut opproc,
                        collid,
                        run.mcx,
                        cst.constvalue,
                        item.values[idx],
                    )?
                };
                matches[i] = result_merge(matches[i], is_or, m.as_bool());
            }
        } else if let Some(saop) = clause.as_scalar_array_op_expr() {
            let opcode = lsyscache::get_opcode(saop.opno)?;
            let mut opproc = fmgr_seams::fmgr_info::call(opcode)?;
            let Some((clause_expr, cst, expronleft)) =
                examine_opclause_args(saop.args.nth(0), saop.args.nth(1))
            else {
                panic!("incompatible clause")
            };
            if !expronleft {
                panic!("incompatible clause");
            }
            let elems = if !cst.constisnull {
                let img = crate::selfuncs::varlena_image_any(run.mcx, cst.constvalue)?;
                let elemtype = arrayfuncs::arr_elemtype(img);
                let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
                Some(arrayfuncs::deconstruct_array(
                    run.mcx, img, elmlen as i32, elmbyval, elmalign as u8, true,
                )?)
            } else {
                None
            };
            let (idx, collid) = mcv_match_expression(clause_expr, keys, sexprs);
            for (i, item) in mcvlist.items.iter().enumerate() {
                let mut m = !saop.useOr;
                if item.isnull[idx] || cst.constisnull {
                    matches[i] = result_merge(matches[i], is_or, false);
                    continue;
                }
                if result_is_final(matches[i], is_or) {
                    continue;
                }
                let (elem_values, elem_nulls) = elems.as_ref().expect("deconstructed array");
                for (j, &elem_value) in elem_values.iter().enumerate() {
                    if elem_nulls[j] {
                        m = result_merge(m, saop.useOr, false);
                        continue;
                    }
                    if result_is_final(m, saop.useOr) {
                        break;
                    }
                    let em = types_fmgr::function_call2_coll_in(
                        &mut opproc,
                        collid,
                        run.mcx,
                        item.values[idx],
                        elem_value,
                    )?;
                    m = result_merge(m, saop.useOr, em.as_bool());
                }
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if let Some(nt) = clause.as_null_test() {
            let arg = nt.arg.expect("NullTest arg");
            let (idx, _) = mcv_match_expression(arg, keys, sexprs);
            use types_nodes::primnodes::NullTestType;
            for (i, item) in mcvlist.items.iter().enumerate() {
                let m = match nt.nulltesttype {
                    NullTestType::IS_NULL => item.isnull[idx],
                    NullTestType::IS_NOT_NULL => !item.isnull[idx],
                };
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if let Some(b) = clause.as_bool_expr() {
            use types_nodes::primnodes::BoolExprType;
            match b.boolop {
                BoolExprType::AND_EXPR | BoolExprType::OR_EXPR => {
                    let sub: Vec<Node<'mcx>> = b.args.iter().collect();
                    let bool_matches = mcv_get_match_bitmap(
                        run,
                        &sub,
                        keys,
                        sexprs,
                        mcvlist,
                        b.boolop == BoolExprType::OR_EXPR,
                    )?;
                    for (i, bm) in bool_matches.iter().enumerate() {
                        matches[i] = result_merge(matches[i], is_or, *bm);
                    }
                }
                BoolExprType::NOT_EXPR => {
                    let sub: Vec<Node<'mcx>> = b.args.iter().collect();
                    let not_matches =
                        mcv_get_match_bitmap(run, &sub, keys, sexprs, mcvlist, false)?;
                    for (i, nm) in not_matches.iter().enumerate() {
                        matches[i] = result_merge(matches[i], is_or, !*nm);
                    }
                }
            }
        } else {
            // Bare boolean Var or boolean-returning expression.
            let (idx, _) = mcv_match_expression(clause, keys, sexprs);
            for (i, item) in mcvlist.items.iter().enumerate() {
                let m = !item.isnull[idx] && item.values[idx].as_bool();
                matches[i] = result_merge(matches[i], is_or, m);
            }
        }
    }

    Ok(matches)
}

fn result_merge(value: bool, is_or: bool, m: bool) -> bool {
    if is_or {
        value || m
    } else {
        value && m
    }
}

fn result_is_final(value: bool, is_or: bool) -> bool {
    if is_or {
        value
    } else {
        !value
    }
}

// dependency_is_compatible_clause (dependencies.c), Var form.
fn dependency_is_compatible_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: RinfoId,
    relid: i32,
) -> PgResult<Option<i16>> {
    {
        let r = run.root.rinfo(rid);
        if r.pseudoconstant {
            return Ok(None);
        }
        if crate::relnode::relids_singleton_member(&r.clause_relids).is_none() {
            return Ok(None);
        }
    }
    let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
    dependency_compatible_node(run, clause, relid)
}

fn dependency_compatible_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
) -> PgResult<Option<i16>> {
    let clause_expr: Node<'mcx>;
    if let Some(op) = clause.as_op_expr() {
        if op.args.len() != 2 {
            return Ok(None);
        }
        if clauses::is_pseudo_constant_clause(op.args.nth(1))? {
            clause_expr = op.args.nth(0);
        } else if clauses::is_pseudo_constant_clause(op.args.nth(0))? {
            clause_expr = op.args.nth(1);
        } else {
            return Ok(None);
        }
        if lsyscache::get_oprrest(op.opno)? != F_EQSEL {
            return Ok(None);
        }
    } else if let Some(saop) = clause.as_scalar_array_op_expr() {
        if !saop.useOr {
            return Ok(None);
        }
        if saop.args.len() != 2 {
            return Ok(None);
        }
        if !clauses::is_pseudo_constant_clause(saop.args.nth(1))? {
            return Ok(None);
        }
        clause_expr = saop.args.nth(0);
        if lsyscache::get_oprrest(saop.opno)? != F_EQSEL {
            return Ok(None);
        }
    } else if let Some(b) = clause.as_bool_expr() {
        use types_nodes::primnodes::BoolExprType;
        match b.boolop {
            BoolExprType::OR_EXPR => {
                let mut attnum: Option<i16> = None;
                for arg in &b.args {
                    let Some(a) = dependency_compatible_node(run, arg, relid)? else {
                        return Ok(None);
                    };
                    match attnum {
                        None => attnum = Some(a),
                        Some(prev) if prev == a => {}
                        _ => return Ok(None),
                    }
                }
                return Ok(attnum);
            }
            BoolExprType::NOT_EXPR => {
                clause_expr = b.args.nth(0);
            }
            BoolExprType::AND_EXPR => return Ok(None),
        }
    } else {
        clause_expr = clause;
    }
    let clause_expr = strip_relabel(clause_expr);
    let Some(var) = clause_expr.as_var() else { return Ok(None) };
    if var.varno != relid || var.varlevelsup != 0 || var.varattno <= 0 {
        return Ok(None);
    }
    Ok(Some(var.varattno))
}

// dependency_is_compatible_expression (dependencies.c): like the clause
// form but matches the operand against dependencies-statistics expressions;
// returns the matching statistics expression.
fn dependency_is_compatible_expression<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: RinfoId,
    dep_stat_exprs: &[Node<'mcx>],
) -> PgResult<Option<Node<'mcx>>> {
    if dep_stat_exprs.is_empty() {
        return Ok(None);
    }
    {
        let r = run.root.rinfo(rid);
        if r.pseudoconstant {
            return Ok(None);
        }
        if crate::relnode::relids_singleton_member(&r.clause_relids).is_none() {
            return Ok(None);
        }
    }
    let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
    dependency_expression_node(run, clause, dep_stat_exprs)
}

fn dependency_expression_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    dep_stat_exprs: &[Node<'mcx>],
) -> PgResult<Option<Node<'mcx>>> {
    let clause_expr: Node<'mcx>;
    if let Some(op) = clause.as_op_expr() {
        if op.args.len() != 2 {
            return Ok(None);
        }
        if clauses::is_pseudo_constant_clause(op.args.nth(1))? {
            clause_expr = op.args.nth(0);
        } else if clauses::is_pseudo_constant_clause(op.args.nth(0))? {
            clause_expr = op.args.nth(1);
        } else {
            return Ok(None);
        }
        if lsyscache::get_oprrest(op.opno)? != F_EQSEL {
            return Ok(None);
        }
    } else if let Some(saop) = clause.as_scalar_array_op_expr() {
        if !saop.useOr {
            return Ok(None);
        }
        if saop.args.len() != 2 {
            return Ok(None);
        }
        if !clauses::is_pseudo_constant_clause(saop.args.nth(1))? {
            return Ok(None);
        }
        clause_expr = saop.args.nth(0);
        if lsyscache::get_oprrest(saop.opno)? != F_EQSEL {
            return Ok(None);
        }
    } else if let Some(b) = clause.as_bool_expr() {
        use types_nodes::primnodes::BoolExprType;
        match b.boolop {
            BoolExprType::OR_EXPR => {
                let mut expr: Option<Node<'mcx>> = None;
                for arg in &b.args {
                    let Some(or_expr) = dependency_expression_node(run, arg, dep_stat_exprs)?
                    else {
                        return Ok(None);
                    };
                    match expr {
                        None => expr = Some(or_expr),
                        Some(prev) if types_nodes::equal(prev, or_expr) => {}
                        _ => return Ok(None),
                    }
                }
                return Ok(expr);
            }
            BoolExprType::NOT_EXPR => {
                clause_expr = b.args.nth(0);
            }
            BoolExprType::AND_EXPR => return Ok(None),
        }
    } else {
        clause_expr = clause;
    }
    let clause_expr = strip_relabel(clause_expr);
    Ok(dep_stat_exprs
        .iter()
        .copied()
        .find(|&se| types_nodes::equal(clause_expr, se)))
}

struct DepItem {
    degree: f64,
    attributes: Vec<i16>,
}

fn find_strongest_dependency(deps: &[DepItem], attnums: &Relids<'_>) -> Option<usize> {
    let nattnums = relids_num_members(attnums);
    let mut strongest: Option<usize> = None;
    for (i, d) in deps.iter().enumerate() {
        if d.attributes.len() as i32 > nattnums {
            continue;
        }
        if let Some(s) = strongest {
            if d.attributes.len() < deps[s].attributes.len() {
                continue;
            }
            if deps[s].attributes.len() == d.attributes.len() && deps[s].degree > d.degree {
                continue;
            }
        }
        if d.attributes.iter().all(|&a| relids_is_member(a as i32, attnums)) {
            strongest = Some(i);
        }
    }
    strongest
}

// dependencies_clauselist_selectivity (dependencies.c). Expressions get
// negative attnums (-1, -2, ...) shifted positive by attnum_offset so the
// bitmap machinery applies uniformly.
#[allow(clippy::too_many_arguments)]
fn dependencies_clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    rel: RelId,
    estimated: &mut [bool],
) -> PgResult<f64> {
    if !has_stats_of_kind(run, rel, STATS_EXT_DEPENDENCIES) {
        return Ok(1.0);
    }
    let relid = run.root.rel(rel).relid as i32;
    let inh = run.rte(relid as usize).inh;

    let dep_stat_ids: Vec<types_pathnodes::NodeId> = run
        .root
        .rel(rel)
        .statlist
        .iter()
        .copied()
        .filter(|&id| run.root.statistic_ext(id).kind == STATS_EXT_DEPENDENCIES)
        .collect();
    let mut dep_stat_exprs: Vec<Node<'mcx>> = Vec::new();
    for &id in &dep_stat_ids {
        dep_stat_exprs.extend(stat_exprs(run, id));
    }

    let mut list_attnums: Vec<Option<i16>> = Vec::with_capacity(clauses.len());
    let mut unique_exprs: Vec<Node<'mcx>> = Vec::new();
    for (i, &rid) in clauses.iter().enumerate() {
        let a = if estimated[i] {
            None
        } else if let Some(a) = dependency_is_compatible_clause(run, rid, relid)? {
            Some(a)
        } else if let Some(expr) =
            dependency_is_compatible_expression(run, rid, &dep_stat_exprs)?
        {
            let idx = match unique_exprs.iter().position(|&e| types_nodes::equal(e, expr)) {
                Some(j) => j,
                None => {
                    unique_exprs.push(expr);
                    unique_exprs.len() - 1
                }
            };
            Some(-((idx + 1) as i16))
        } else {
            None
        };
        list_attnums.push(a);
    }

    let attnum_offset: i16 =
        if unique_exprs.is_empty() { 0 } else { unique_exprs.len() as i16 + 1 };

    let mut clauses_attnums: Relids<'mcx> = crate::relnode::relids_empty();
    for a in list_attnums.iter_mut() {
        if let Some(v) = a {
            *v += attnum_offset;
            let single = crate::relnode::relids_singleton(run.mcx, *v as u32);
            clauses_attnums = crate::relnode::relids_union(run.mcx, &clauses_attnums, &single);
        }
    }

    if relids_num_members(&clauses_attnums) < 2 {
        return Ok(1.0);
    }

    // Load dependencies from stats matching >= 2 clause attnums/expressions;
    // remap expression attnums to the unique-expression numbering and drop
    // items not fully covered by clauses.
    let mut deps: Vec<DepItem> = Vec::new();
    for id in dep_stat_ids {
        let (stat_inh, stat_oid, keys) = {
            let info = run.root.statistic_ext(id);
            (info.inherit, info.stat_oid, clone_relids(run, &info.keys))
        };
        if stat_inh != inh {
            continue;
        }
        let this_stat_exprs = stat_exprs(run, id);
        let mut nmatched = 0;
        for (i, w) in crate::relnode::relids_word_slice(&keys).iter().enumerate() {
            let mut w = *w;
            while w != 0 {
                let m = (i * 64) as i32 + w.trailing_zeros() as i32;
                if relids_is_member(m + attnum_offset as i32, &clauses_attnums) {
                    nmatched += 1;
                }
                w &= w - 1;
            }
        }
        let mut nexprs = 0;
        for &ue in &unique_exprs {
            for &se in &this_stat_exprs {
                if types_nodes::equal(se, ue) {
                    nexprs += 1;
                }
            }
        }
        if nmatched + nexprs < 2 {
            continue;
        }
        let img = syscache_seams::statext_data_blob::call(
            run.mcx,
            stat_oid,
            inh,
            Anum_data_stxddependencies,
        )?
        .unwrap_or_else(|| {
            panic!("requested statistics kind \"f\" is not yet built for statistics object {stat_oid}")
        });
        let loaded = statistics::dependencies::statext_dependencies_deserialize(run.mcx, &img[4..])?;
        'dep: for d in loaded.deps.iter() {
            let mut attrs: Vec<i16> = Vec::with_capacity(d.attributes.len());
            for &a in d.attributes.iter() {
                if a > 0 {
                    let shifted = a + attnum_offset;
                    if !relids_is_member(shifted as i32, &clauses_attnums) {
                        continue 'dep;
                    }
                    attrs.push(shifted);
                } else {
                    let idx = (-(1 + a)) as usize;
                    let expr = this_stat_exprs[idx];
                    let Some(m) =
                        unique_exprs.iter().position(|&ue| types_nodes::equal(ue, expr))
                    else {
                        continue 'dep;
                    };
                    attrs.push(-((m + 1) as i16) + attnum_offset);
                }
            }
            deps.push(DepItem { degree: d.degree, attributes: attrs });
        }
    }
    if deps.is_empty() {
        return Ok(1.0);
    }

    let mut applied: Vec<usize> = Vec::new();
    let mut remaining = clone_relids(run, &clauses_attnums);
    while let Some(di) = find_strongest_dependency(&deps, &remaining) {
        applied.push(di);
        let implied = *deps[di].attributes.last().expect("dependency attributes");
        remaining = relids_del_member(run, &remaining, implied as i32);
    }
    if applied.is_empty() {
        return Ok(1.0);
    }

    // clauselist_apply_dependencies (dependencies.c).
    let mut attnums: Vec<i16> = Vec::new();
    for &di in &applied {
        for &a in &deps[di].attributes {
            if !attnums.contains(&a) {
                attnums.push(a);
            }
        }
    }
    attnums.sort_unstable();

    let mut attr_sel: Vec<f64> = Vec::with_capacity(attnums.len());
    for &a in &attnums {
        let mut attr_clauses: Vec<RinfoId> = Vec::new();
        for (i, &rid) in clauses.iter().enumerate() {
            if list_attnums[i] == Some(a) {
                attr_clauses.push(rid);
                estimated[i] = true;
            }
        }
        let s = crate::clausesel::clauselist_selectivity_ext(
            run,
            &attr_clauses,
            varrelid,
            jointype,
            sjinfo,
            false,
        )?;
        attr_sel.push(s);
    }

    for &di in applied.iter().rev() {
        let dep = &deps[di];
        let mut s1 = 1.0f64;
        for &a in &dep.attributes[..dep.attributes.len() - 1] {
            let idx = attnums.binary_search(&a).expect("implying attnum");
            s1 *= attr_sel[idx];
        }
        let implied = *dep.attributes.last().unwrap();
        let idx = attnums.binary_search(&implied).expect("implied attnum");
        let s2 = attr_sel[idx];
        let f = dep.degree;
        attr_sel[idx] = if s1 <= s2 { f + (1.0 - f) * s2 } else { f * s2 / s1 + (1.0 - f) * s2 };
    }

    let mut s1 = 1.0f64;
    for s in attr_sel {
        s1 *= s;
    }
    Ok(clamp_probability(s1))
}

fn relids_del_member<'mcx>(
    run: &PlannerRun<'mcx>,
    r: &Relids<'mcx>,
    x: i32,
) -> Relids<'mcx> {
    let mut cloned = clone_relids(run, r);
    if let Some(w) = crate::relnode::relids_word_slice_mut(&mut cloned).get_mut(x as usize / 64) {
        *w &= !(1u64 << (x % 64));
    }
    cloned
}

// estimate_multivariate_ndistinct (selfuncs.c): pick the ndistinct
// statistics object matching the most vars/expressions, find the exact
// item covering the match, and return its estimate plus a consumed-mask
// parallel to `nodes`.
pub fn estimate_multivariate_ndistinct<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    nodes: &[Node<'mcx>],
) -> PgResult<Option<(f64, Vec<bool>)>> {
    if run.root.rel(rel).statlist.is_empty() {
        return Ok(None);
    }
    let relid = run.root.rel(rel).relid as i32;
    let inh = run.rte(relid as usize).inh;

    let mut nmatches_vars = 0i32;
    let mut nmatches_exprs = 0i32;
    let mut best: Option<types_pathnodes::NodeId> = None;
    let statlist: Vec<types_pathnodes::NodeId> =
        run.root.rel(rel).statlist.iter().copied().collect();
    for id in statlist {
        {
            let info = run.root.statistic_ext(id);
            if info.kind != STATS_EXT_NDISTINCT || info.inherit != inh {
                continue;
            }
        }
        let sexprs = stat_exprs(run, id);
        let info = run.root.statistic_ext(id);
        let mut nshared_vars = 0i32;
        let mut nshared_exprs = 0i32;
        for &node in nodes {
            if let Some(v) = node.as_var() {
                if v.varattno <= 0 {
                    continue;
                }
                if relids_is_member(v.varattno as i32, &info.keys) {
                    nshared_vars += 1;
                }
                continue;
            }
            if sexprs.iter().any(|&se| types_nodes::equal(node, se)) {
                nshared_exprs += 1;
            }
        }
        if nshared_vars + nshared_exprs < 2 {
            continue;
        }
        if nshared_exprs > nmatches_exprs
            || (nshared_exprs == nmatches_exprs && nshared_vars > nmatches_vars)
        {
            best = Some(id);
            nmatches_vars = nshared_vars;
            nmatches_exprs = nshared_exprs;
        }
    }
    let Some(matched_id) = best else { return Ok(None) };
    let matched_exprs = stat_exprs(run, matched_id);
    let (stat_oid, matched_keys) = {
        let info = run.root.statistic_ext(matched_id);
        (info.stat_oid, clone_relids(run, &info.keys))
    };

    let img = syscache_seams::statext_data_blob::call(
        run.mcx,
        stat_oid,
        inh,
        Anum_data_stxdndistinct,
    )?
    .unwrap_or_else(|| {
        panic!("requested statistics kind \"d\" is not yet built for statistics object {stat_oid}")
    });
    let nd = statistics::mvdistinct::statext_ndistinct_deserialize(run.mcx, &img[4..])?;

    let attnum_offset: i32 =
        if matched_exprs.is_empty() { 0 } else { matched_exprs.len() as i32 + 1 };

    let mut matched: Vec<i32> = Vec::new();
    for &node in nodes {
        let mut found = false;
        if let Some(v) = node.as_var() {
            if v.varattno <= 0 {
                continue;
            }
            if !relids_is_member(v.varattno as i32, &matched_keys) {
                continue;
            }
            let a = v.varattno as i32 + attnum_offset;
            if !matched.contains(&a) {
                matched.push(a);
            }
            found = true;
        }
        if found {
            continue;
        }
        for (idx, &se) in matched_exprs.iter().enumerate() {
            if types_nodes::equal(node, se) {
                let a = -((idx + 1) as i32) + attnum_offset;
                if !matched.contains(&a) {
                    matched.push(a);
                }
                break;
            }
        }
    }

    let mut item_ndistinct: Option<f64> = None;
    for item in nd.items.iter() {
        if item.attributes.len() != matched.len() {
            continue;
        }
        if item
            .attributes
            .iter()
            .all(|&a| matched.contains(&(a as i32 + attnum_offset)))
        {
            item_ndistinct = Some(item.ndistinct);
            break;
        }
    }
    let Some(ndistinct) = item_ndistinct else { panic!("corrupt MVNDistinct entry") };

    let mut consumed: Vec<bool> = Vec::with_capacity(nodes.len());
    for &node in nodes {
        if let Some(v) = node.as_var() {
            if v.varattno <= 0 {
                consumed.push(false);
                continue;
            }
            consumed.push(matched.contains(&(v.varattno as i32 + attnum_offset)));
            continue;
        }
        consumed.push(matched_exprs.iter().any(|&se| types_nodes::equal(node, se)));
    }
    Ok(Some((ndistinct, consumed)))
}
