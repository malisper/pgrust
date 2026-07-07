use mcx::PgVec;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_nodes::{Node, Var};
use types_pathnodes::{RelId, RinfoId};

use crate::pathnode::add_path;
use crate::relnode::relids_is_member;
use crate::run::PlannerRun;

const TID_EQUAL_OPERATOR: Oid = 387;
const TID_LESS_OPERATOR: Oid = 2799;
const TID_GREATER_OPERATOR: Oid = 2800;
const TID_LESS_EQ_OPERATOR: Oid = 2801;
const TID_GREATER_EQ_OPERATOR: Oid = 2802;

fn is_ctid_var(v: &Var<'_>, rel_relid: u32) -> bool {
    v.varattno == types_tuple::htup::SelfItemPointerAttributeNumber as i16
        && v.vartype == types_core::catalog::TIDOID
        && v.varno == rel_relid as i32
        && v.varnullingrels.is_empty()
        && v.varlevelsup == 0
}

fn is_binary_tid_clause(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    let rel_relid = run.root.rel(rel).relid;
    let rinfo = run.root.rinfo(rid);
    let clause = *run.root.expr_node(rinfo.clause);
    let Some(op) = clause.as_op_expr() else {
        return Ok(false);
    };
    if op.args.len() != 2 {
        return Ok(false);
    }
    let arg1 = op.args.nth(0);
    let arg2 = op.args.nth(1);

    let (other, other_relids) = if arg1.as_var().is_some_and(|v| is_ctid_var(v, rel_relid)) {
        (arg2, &rinfo.right_relids)
    } else if arg2.as_var().is_some_and(|v| is_ctid_var(v, rel_relid)) {
        (arg1, &rinfo.left_relids)
    } else {
        return Ok(false);
    };

    if relids_is_member(rel_relid as i32, other_relids)
        || clauses::contain_volatile_functions(other)?
    {
        return Ok(false);
    }
    Ok(true)
}

fn opexpr_opno(run: &PlannerRun<'_>, rid: RinfoId) -> Oid {
    run.root
        .expr_node(run.root.rinfo(rid).clause)
        .as_op_expr()
        .map_or(0, |op| op.opno)
}

fn is_tid_equal_clause(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    Ok(is_binary_tid_clause(run, rid, rel)? && opexpr_opno(run, rid) == TID_EQUAL_OPERATOR)
}

fn is_tid_range_clause(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    if !is_binary_tid_clause(run, rid, rel)? {
        return Ok(false);
    }
    let opno = opexpr_opno(run, rid);
    Ok(opno == TID_LESS_OPERATOR
        || opno == TID_LESS_EQ_OPERATOR
        || opno == TID_GREATER_OPERATOR
        || opno == TID_GREATER_EQ_OPERATOR)
}

fn is_tid_equal_any_clause(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    let rel_relid = run.root.rel(rel).relid;
    let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
    let Some(saop) = clause.as_scalar_array_op_expr() else {
        return Ok(false);
    };
    if saop.opno != TID_EQUAL_OPERATOR || !saop.useOr {
        return Ok(false);
    }
    debug_assert_eq!(saop.args.len(), 2);
    let arg1 = saop.args.nth(0);
    let arg2 = saop.args.nth(1);

    if !arg1.as_var().is_some_and(|v| is_ctid_var(v, rel_relid)) {
        return Ok(false);
    }
    let varnos = vars::pull_varnos(run.mcx, arg2)?;
    if varnos.is_member(rel_relid as i32) || clauses::contain_volatile_functions(arg2)? {
        return Ok(false);
    }
    Ok(true)
}

fn is_current_of_clause(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> bool {
    let rel_relid = run.root.rel(rel).relid;
    run.root
        .expr_node(run.root.rinfo(rid).clause)
        .as_current_of_expr()
        .is_some_and(|c| c.cvarno == rel_relid)
}

fn restriction_is_securely_promotable(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> bool {
    let r = run.root.rinfo(rid);
    r.security_level <= run.root.rel(rel).baserestrict_min_security || r.leakproof
}

fn restrict_info_is_tid_qual(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    if run.root.rinfo(rid).pseudoconstant {
        return Ok(false);
    }
    if !restriction_is_securely_promotable(run, rid, rel) {
        return Ok(false);
    }
    Ok(is_tid_equal_clause(run, rid, rel)?
        || is_tid_equal_any_clause(run, rid, rel)?
        || is_current_of_clause(run, rid, rel))
}

// TidQualFromRestrictInfoList (tidpath.c). Result has implicit OR semantics.
// OR sub-clauses arrive as bare exprs (orclause sub-rinfos are never built in
// this repo); or_arm_rinfo synthesizes the rinfo C prebuilt.
fn tid_qual_from_restrict_info_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rlist: &[RinfoId],
    rel: RelId,
) -> PgResult<(PgVec<'mcx, RinfoId>, bool)> {
    let mcx = run.mcx;
    let mut tidclause: Option<RinfoId> = None;
    let mut orlist: Option<PgVec<'mcx, RinfoId>> = None;

    for &rid in rlist {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if clauses::is_orclause(clause) {
            let mut rlst: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
            let mut broke = false;
            let mut orargs: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
            orargs.extend(clause.as_bool_expr().expect("OR clause").args.iter());

            for &orarg in orargs.iter() {
                let sublist: PgVec<'mcx, RinfoId>;
                if clauses::is_andclause(orarg) {
                    let mut andrids: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
                    for a in &orarg.as_bool_expr().expect("AND clause").args {
                        andrids.push(crate::indxpath::or_arm_rinfo(run, rid, a)?);
                    }
                    let (sub, sub_is_current_of) =
                        tid_qual_from_restrict_info_list(run, &andrids, rel)?;
                    if sub_is_current_of {
                        return Err(PgError::error(
                            "IS CURRENT OF within OR clause".to_string(),
                        )
                        .into());
                    }
                    sublist = sub;
                } else {
                    let ri = crate::indxpath::or_arm_rinfo(run, rid, orarg)?;
                    debug_assert!(!clauses::is_orclause(orarg), "unflattened OR");
                    if restrict_info_is_tid_qual(run, ri, rel)? {
                        let mut v = PgVec::new_in(mcx);
                        v.push(ri);
                        sublist = v;
                    } else {
                        sublist = PgVec::new_in(mcx);
                    }
                }

                if sublist.is_empty() {
                    rlst.clear();
                    broke = true;
                    break;
                }
                rlst.extend(sublist.iter().copied());
            }

            if !broke
                && !rlst.is_empty()
                && orlist.as_ref().is_none_or(|o| rlst.len() < o.len())
            {
                orlist = Some(rlst);
            }
        } else if restrict_info_is_tid_qual(run, rid, rel)? {
            if is_current_of_clause(run, rid, rel) {
                let mut v = PgVec::new_in(mcx);
                v.push(rid);
                return Ok((v, true));
            }
            if tidclause.is_none() {
                tidclause = Some(rid);
            }
        }
    }

    // Prefer any singleton CTID qual to an OR'ed list.
    if let Some(tc) = tidclause {
        let mut v = PgVec::new_in(mcx);
        v.push(tc);
        return Ok((v, false));
    }
    Ok((orlist.unwrap_or_else(|| PgVec::new_in(mcx)), false))
}

// TidRangeQualFromRestrictInfoList (tidpath.c). Implicit AND semantics.
fn tid_range_qual_from_restrict_info_list<'mcx>(
    run: &PlannerRun<'mcx>,
    rlist: &[RinfoId],
    rel: RelId,
) -> PgResult<PgVec<'mcx, RinfoId>> {
    let mut rlst: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
    if run.root.rel(rel).amflags & crate::plancat::AMFLAG_HAS_TID_RANGE == 0 {
        return Ok(rlst);
    }
    for &rid in rlist {
        if is_tid_range_clause(run, rid, rel)? {
            rlst.push(rid);
        }
    }
    Ok(rlst)
}

// BuildParameterizedTidPaths (tidpath.c). Only TidEqual join clauses are
// considered; the validity checks must match restrict_info_is_tid_qual.
fn build_parameterized_tid_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
) -> PgResult<()> {
    use types_pathnodes::relids::{relids_del_member, relids_union};
    let mcx = run.mcx;
    for &rid in clauses {
        if run.root.rinfo(rid).pseudoconstant
            || !restriction_is_securely_promotable(run, rid, rel)
            || !is_tid_equal_clause(run, rid, rel)?
        {
            continue;
        }
        if !crate::indxpath::join_clause_is_movable_to(run, rid, rel) {
            continue;
        }
        let mut tidquals: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        tidquals.push(rid);
        let required_outer = relids_union(
            mcx,
            &run.root.rinfo(rid).required_relids,
            &run.root.rel(rel).lateral_relids,
        );
        let required_outer =
            relids_del_member(mcx, &required_outer, run.root.rel(rel).relid as i32);
        let path = crate::pathnode::create_tidscan_path(run, rel, tidquals, &required_outer)?;
        add_path(run, rel, path);
    }
    Ok(())
}

// ec_member_matches_ctid (tidpath.c).
fn ec_member_matches_ctid(
    run: &PlannerRun<'_>,
    rel: RelId,
    _ec: types_pathnodes::EcId,
    em: types_pathnodes::EmId,
) -> bool {
    run.root
        .expr_node(run.root.em(em).em_expr)
        .as_var()
        .is_some_and(|v| is_ctid_var(v, run.root.rel(rel).relid))
}

// create_tidscan_paths (tidpath.c). True = CurrentOf path forced; caller adds
// no other paths.
pub fn create_tidscan_paths<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<bool> {
    let mcx = run.mcx;
    // C walks baserestrictinfo, joininfo, and the rel's ECs unconditionally;
    // one over-inclusive ctid probe gates those walks and the RinfoId copies
    // C never makes — any OR or ctid-shaped clause, ctid-equal join clause,
    // or ctid EC member falls through to the full builders, so path
    // generation is unchanged.
    let mut maybe_tid = false;
    for &rid in run.root.rel(rel).baserestrictinfo.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if clauses::is_orclause(clause)
            || is_binary_tid_clause(run, rid, rel)?
            || is_tid_equal_any_clause(run, rid, rel)?
            || is_current_of_clause(run, rid, rel)
        {
            maybe_tid = true;
            break;
        }
    }
    if !maybe_tid {
        for i in 0..run.root.rel(rel).joininfo.len() {
            let rid = run.root.rel(rel).joininfo[i];
            if is_tid_equal_clause(run, rid, rel)? {
                maybe_tid = true;
                break;
            }
        }
    }
    if !maybe_tid && run.root.rel(rel).has_eclass_joins {
        let eclass_indexes = types_pathnodes::relids::relids_copy(
            mcx,
            &run.root.rel(rel).eclass_indexes,
        );
        'ecs: for i in types_pathnodes::relids::relids_members(&eclass_indexes) {
            let ec = types_pathnodes::EcId(i as u32);
            for m in 0..run.root.ec(ec).ec_members.len() {
                let em = run.root.ec(ec).ec_members[m];
                if ec_member_matches_ctid(run, rel, ec, em) {
                    maybe_tid = true;
                    break 'ecs;
                }
            }
        }
    }
    if !maybe_tid {
        return Ok(false);
    }

    let mut baserestrictinfo: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    baserestrictinfo.extend(run.root.rel(rel).baserestrictinfo.iter().copied());

    let (tidquals, is_current_of) =
        tid_qual_from_restrict_info_list(run, &baserestrictinfo, rel)?;

    if !tidquals.is_empty() && (crate::costsize::gucs::enable_tidscan() || is_current_of) {
        // No join clauses, but LATERAL refs in the tlist can still require
        // parameterization.
        let required_outer =
            types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        let path = crate::pathnode::create_tidscan_path(run, rel, tidquals, &required_outer)?;
        add_path(run, rel, path);
        if is_current_of {
            return Ok(true);
        }
    }

    if !crate::costsize::gucs::enable_tidscan() {
        return Ok(false);
    }

    let tidrangequals = tid_range_qual_from_restrict_info_list(run, &baserestrictinfo, rel)?;
    if !tidrangequals.is_empty() {
        let required_outer =
            types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        let path =
            crate::pathnode::create_tidrangescan_path(run, rel, tidrangequals, &required_outer)?;
        add_path(run, rel, path);
    }

    // Parameterized TidPaths from EC-derived equalities: simple
    // "t1.ctid = t2.ctid" clauses turn into ECs.
    if run.root.rel(rel).has_eclass_joins {
        let lateral_referencers =
            types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).lateral_referencers);
        let clauses = crate::equivclass::generate_implied_equalities_for_column(
            run,
            rel,
            ec_member_matches_ctid,
            &lateral_referencers,
        )?;
        build_parameterized_tid_paths(run, rel, &clauses)?;
    }

    // "Loose" join quals, e.g. ctid equalities that are outer join quals.
    let joininfo =
        types_pathnodes::relids::pgvec_clone_shallow(mcx, &run.root.rel(rel).joininfo);
    build_parameterized_tid_paths(run, rel, &joininfo)?;

    Ok(false)
}
