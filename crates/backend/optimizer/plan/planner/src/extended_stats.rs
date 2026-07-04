//! Planner consumption of extended statistics (extended_stats.c's
//! statext_clauselist_selectivity + dependencies.c/mcv.c selectivity legs +
//! plancat.c's get_relation_statistics). Expression statistics are
//! structurally absent (plancat panics on stxexprs).

use mcx::PgVec;
// Scratch on the stats-present path uses std Vec where element types carry
// lifetimes awkwardly; per-query, bounded by clause/statistics counts.
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    JoinType, RelId, Relids, RinfoId, SpecialJoinInfo, StatisticExtInfo,
};

use crate::relnode::{relids_is_member, relids_is_subset, relids_num_members};
use crate::run::PlannerRun;

const STATS_EXT_NDISTINCT: i8 = b'd' as i8;
const STATS_EXT_DEPENDENCIES: i8 = b'f' as i8;
const STATS_EXT_MCV: i8 = b'm' as i8;
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
    let mut r: Relids<'mcx> = None;
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
    for &statoid in statoids.iter() {
        let form = syscache_seams::statext_form::call(mcx, statoid)?
            .unwrap_or_else(|| panic!("cache lookup failed for statistics object {statoid}"));
        if form.has_exprs {
            panic!("get_relation_statistics (plancat.c): expression statistics lane");
        }
        let keys = attnums_from_members(run, &form.keys);
        for inh in [true, false] {
            let Some((nd, deps, mcv, exprs)) =
                syscache_seams::statext_data_kinds::call(statoid, inh)?
            else {
                continue;
            };
            if exprs {
                panic!("get_relation_statistics (plancat.c): expression statistics lane");
            }
            for (built, kind) in [
                (nd, STATS_EXT_NDISTINCT),
                (deps, STATS_EXT_DEPENDENCIES),
                (mcv, STATS_EXT_MCV),
            ] {
                if !built || !form.kinds.contains(&(kind as u8)) {
                    continue;
                }
                let info = StatisticExtInfo {
                    stat_oid: statoid,
                    inherit: inh,
                    rel: Some(rel),
                    kind,
                    keys: clone_relids(run, &keys),
                    exprs: PgVec::new_in(mcx),
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

// statext_clauselist_selectivity (extended_stats.c), AND-list leg (the OR
// entry is unwired: OR clauses take the per-clause path, as before).
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

// statext_is_compatible_clause_internal (bare node).
fn compatible_internal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    relid: i32,
    attnums: &mut Relids<'mcx>,
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
            return compatible_internal(run, expr, relid, attnums, leakproof);
        }
        return Ok(false);
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
            return compatible_internal(run, expr, relid, attnums, leakproof);
        }
        return Ok(false);
    }

    if let Some(b) = clause.as_bool_expr() {
        for arg in &b.args {
            if !compatible_internal(run, arg, relid, attnums, leakproof)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    if let Some(nt) = clause.as_null_test() {
        let arg = nt.arg.expect("NullTest arg");
        if arg.as_var().is_some() {
            return compatible_internal(run, arg, relid, attnums, leakproof);
        }
        return Ok(false);
    }

    // No expression statistics: any other shape is incompatible.
    Ok(false)
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
) -> PgResult<Option<Relids<'mcx>>> {
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
    let mut attnums: Relids<'mcx> = None;
    let mut leakproof = true;
    if !compatible_internal(run, clause, relid, &mut attnums, &mut leakproof)? {
        return Ok(None);
    }
    // Non-leakproof operators may reveal MCV values; require every row of
    // the referenced columns to be readable.
    if !leakproof {
        let mut cols: PgVec<'_, i16> = PgVec::new_in(run.mcx);
        cols.extend(crate::relnode::relids_members(&attnums).map(|a| a as i16));
        if !crate::selfuncs::all_rows_selectable(run, &run.root, relid, Some(&cols))? {
            return Ok(None);
        }
    }
    Ok(Some(attnums))
}

// choose_best_statistics (extended_stats.c), no-expressions form.
fn choose_best_statistics(
    run: &PlannerRun<'_>,
    rel: RelId,
    requiredkind: i8,
    inh: bool,
    clause_attnums: &[Option<Relids<'_>>],
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut best_num_matched = 2;
    let mut best_match_keys = STATS_MAX_DIMENSIONS as i32 + 1;
    for (si, &id) in run.root.rel(rel).statlist.iter().enumerate() {
        let info = run.root.statistic_ext(id);
        if info.kind != requiredkind || info.inherit != inh {
            continue;
        }
        let mut matched: Vec<i32> = Vec::new();
        for ca in clause_attnums.iter() {
            let Some(ca) = ca else { continue };
            if !relids_is_subset(ca, &info.keys) {
                continue;
            }
            if let Some(b) = ca {
                for (i, w) in b.word_slice().iter().enumerate() {
                    let mut w = *w;
                    while w != 0 {
                        let m = (i * 64) as i32 + w.trailing_zeros() as i32;
                        if !matched.contains(&m) {
                            matched.push(m);
                        }
                        w &= w - 1;
                    }
                }
            }
        }
        let num_matched = matched.len() as i32;
        let numkeys = relids_num_members(&info.keys);
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

    let mut list_attnums: Vec<Option<Relids<'mcx>>> = Vec::with_capacity(clauses.len());
    for (i, &rid) in clauses.iter().enumerate() {
        if estimated[i] {
            list_attnums.push(None);
        } else {
            list_attnums.push(statext_is_compatible_clause(run, rid, relid)?);
        }
    }

    loop {
        let Some(si) = choose_best_statistics(run, rel, STATS_EXT_MCV, inh, &list_attnums)
        else {
            break;
        };
        let stat_id = run.root.rel(rel).statlist[si];
        let (stat_oid, stat_keys) = {
            let info = run.root.statistic_ext(stat_id);
            (info.stat_oid, clone_relids(run, &info.keys))
        };

        let mut stat_clauses: Vec<RinfoId> = Vec::new();
        for (i, &rid) in clauses.iter().enumerate() {
            let Some(ca) = &list_attnums[i] else { continue };
            if !relids_is_subset(ca, &stat_keys) {
                continue;
            }
            stat_clauses.push(rid);
            estimated[i] = true;
            list_attnums[i] = None;
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
            mcv_clauselist_selectivity(run, stat_oid, inh, &stat_keys, &stat_clauses)?;
        let stat_sel = mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel);
        sel *= stat_sel;
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
    clauses: &[RinfoId],
) -> PgResult<(f64, f64, f64)> {
    let mcv = load_mcv(run, stat_oid, inh)?;
    let nodes: Vec<Node<'mcx>> = clauses
        .iter()
        .map(|&rid| *run.root.expr_node(run.root.rinfo(rid).clause))
        .collect();
    let matches = mcv_get_match_bitmap(run, &nodes, keys, &mcv, false)?;
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
    let Some(b) = keys else { panic!("mcv_match_expression: empty keys") };
    let mut idx = 0usize;
    for (i, w) in b.word_slice().iter().enumerate() {
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

// mcv_get_match_bitmap (mcv.c); expression arms unreachable (stats have no
// expressions).
fn mcv_get_match_bitmap<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    keys: &Relids<'mcx>,
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
            let var = clause_expr.as_var().expect("statistics clause Var");
            let collid = var.varcollid;
            let idx = bms_member_index(keys, var.varattno);
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
                    types_fmgr::function_call2_coll(
                        &mut opproc,
                        collid,
                        item.values[idx],
                        cst.constvalue,
                    )?
                } else {
                    types_fmgr::function_call2_coll(
                        &mut opproc,
                        collid,
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
                let p = cst.constvalue.as_usize() as *const u8;
                // SAFETY: non-null array datum; planner consts carry inline
                // 4-byte headers (as scalararraysel).
                let b0 = unsafe { *p };
                assert!(
                    b0 != 0x01 && b0 & 0x03 == 0,
                    "mcv_get_match_bitmap (mcv.c): toasted/packed array const"
                );
                // SAFETY: 4-byte varlena header verified; image is VARSIZE bytes.
                let img = unsafe {
                    core::slice::from_raw_parts(
                        p,
                        arrayfuncs::arr_size(core::slice::from_raw_parts(p, 4)),
                    )
                };
                let elemtype = arrayfuncs::arr_elemtype(img);
                let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
                Some(arrayfuncs::deconstruct_array(
                    run.mcx, img, elmlen as i32, elmbyval, elmalign as u8, true,
                )?)
            } else {
                None
            };
            let var = clause_expr.as_var().expect("statistics clause Var");
            let collid = var.varcollid;
            let idx = bms_member_index(keys, var.varattno);
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
                    let em = types_fmgr::function_call2_coll(
                        &mut opproc,
                        collid,
                        item.values[idx],
                        elem_value,
                    )?;
                    m = result_merge(m, saop.useOr, em.as_bool());
                }
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else if let Some(nt) = clause.as_null_test() {
            let arg = nt.arg.expect("NullTest arg");
            let var = arg.as_var().expect("statistics NullTest Var");
            let idx = bms_member_index(keys, var.varattno);
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
                        mcvlist,
                        b.boolop == BoolExprType::OR_EXPR,
                    )?;
                    for (i, bm) in bool_matches.iter().enumerate() {
                        matches[i] = result_merge(matches[i], is_or, *bm);
                    }
                }
                BoolExprType::NOT_EXPR => {
                    let sub: Vec<Node<'mcx>> = b.args.iter().collect();
                    let not_matches = mcv_get_match_bitmap(run, &sub, keys, mcvlist, false)?;
                    for (i, nm) in not_matches.iter().enumerate() {
                        matches[i] = result_merge(matches[i], is_or, !*nm);
                    }
                }
            }
        } else if let Some(var) = clause.as_var() {
            let idx = bms_member_index(keys, var.varattno);
            for (i, item) in mcvlist.items.iter().enumerate() {
                let m = !item.isnull[idx] && item.values[idx].as_bool();
                matches[i] = result_merge(matches[i], is_or, m);
            }
        } else {
            panic!("mcv_get_match_bitmap (mcv.c): unsupported clause {:?}", clause.node_tag());
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

// dependency_is_compatible_clause (dependencies.c), Var-only form.
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

// dependencies_clauselist_selectivity (dependencies.c), no-expressions form.
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

    let mut list_attnums: Vec<Option<i16>> = Vec::with_capacity(clauses.len());
    let mut clauses_attnums: Relids<'mcx> = None;
    for (i, &rid) in clauses.iter().enumerate() {
        let a = if estimated[i] {
            None
        } else {
            dependency_is_compatible_clause(run, rid, relid)?
        };
        if let Some(a) = a {
            let single = crate::relnode::relids_singleton(run.mcx, a as u32);
            clauses_attnums = crate::relnode::relids_union(run.mcx, &clauses_attnums, &single);
        }
        list_attnums.push(a);
    }

    if relids_num_members(&clauses_attnums) < 2 {
        return Ok(1.0);
    }

    // Load dependencies from stats matching >= 2 clause attnums, dropping
    // items not fully covered by clauses.
    let mut deps: Vec<DepItem> = Vec::new();
    let statlist: Vec<types_pathnodes::NodeId> =
        run.root.rel(rel).statlist.iter().copied().collect();
    for id in statlist {
        let (kind, stat_inh, stat_oid, keys) = {
            let info = run.root.statistic_ext(id);
            (info.kind, info.inherit, info.stat_oid, clone_relids(run, &info.keys))
        };
        if kind != STATS_EXT_DEPENDENCIES || stat_inh != inh {
            continue;
        }
        let mut nmatched = 0;
        if let Some(b) = &keys {
            for (i, w) in b.word_slice().iter().enumerate() {
                let mut w = *w;
                while w != 0 {
                    let m = (i * 64) as i32 + w.trailing_zeros() as i32;
                    if relids_is_member(m, &clauses_attnums) {
                        nmatched += 1;
                    }
                    w &= w - 1;
                }
            }
        }
        if nmatched < 2 {
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
        for d in loaded.deps.iter() {
            if d.attributes.iter().all(|&a| relids_is_member(a as i32, &clauses_attnums)) {
                deps.push(DepItem { degree: d.degree, attributes: d.attributes.to_vec() });
            }
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
    let cloned = clone_relids(run, r);
    if let Some(mut b) = cloned {
        if let Some(w) = b.word_slice_mut().get_mut(x as usize / 64) {
            *w &= !(1u64 << (x % 64));
        }
        return Some(b);
    }
    None
}

// estimate_multivariate_ndistinct (extended_stats.c): pick the ndistinct
// statistics object covering the most GROUP BY attnums; returns the item's
// estimate and the covered attnums.
pub fn estimate_multivariate_ndistinct<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    varattnos: &[i16],
) -> PgResult<Option<(f64, Vec<i16>)>> {
    if run.root.rel(rel).statlist.is_empty() || varattnos.len() < 2 {
        return Ok(None);
    }
    let relid = run.root.rel(rel).relid as i32;
    let inh = run.rte(relid as usize).inh;

    let mut attnums: Relids<'mcx> = None;
    for &a in varattnos {
        if a <= 0 {
            continue;
        }
        let single = crate::relnode::relids_singleton(run.mcx, a as u32);
        attnums = crate::relnode::relids_union(run.mcx, &attnums, &single);
    }

    let mut best: Option<(types_core::Oid, Relids<'mcx>, i32)> = None;
    let statlist: Vec<types_pathnodes::NodeId> =
        run.root.rel(rel).statlist.iter().copied().collect();
    for id in statlist {
        let info = run.root.statistic_ext(id);
        if info.kind != STATS_EXT_NDISTINCT || info.inherit != inh {
            continue;
        }
        let shared = crate::relnode::relids_intersect(run.mcx, &info.keys, &attnums);
        let nshared = relids_num_members(&shared);
        if nshared < 2 {
            continue;
        }
        let better = match &best {
            None => true,
            Some((_, _, bm)) => nshared > *bm,
        };
        if better {
            let oid = info.stat_oid;
            let keys = clone_relids(run, &info.keys);
            let _ = keys;
            best = Some((oid, shared, nshared));
        }
    }
    let Some((stat_oid, matched, nmatched)) = best else { return Ok(None) };

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

    for item in nd.items.iter() {
        if item.attributes.len() as i32 != nmatched {
            continue;
        }
        if item.attributes.iter().all(|&a| relids_is_member(a as i32, &matched)) {
            let covered: Vec<i16> = item.attributes.to_vec();
            return Ok(Some((item.ndistinct, covered)));
        }
    }
    panic!("corrupt MVNDistinct entry");
}
