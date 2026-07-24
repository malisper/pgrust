//! clausesel.c: clauselist_selectivity with range-clause pairing. Extended
//! stats structurally absent (statlist asserted empty at plancat); orclause
//! memoization unmodeled — bare-node recursion, same numerics (initsplan.rs).

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{equal, Node, NodeTag};
use types_pathnodes::{JoinType, NodeId, RinfoId, SpecialJoinInfo, JOIN_INNER};

use crate::relnode::relids_is_member;
use crate::run::PlannerRun;
use crate::selfuncs::DEFAULT_INEQ_SEL;

const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;
const F_SCALARLTSEL: u32 = 103;
const F_SCALARGTSEL: u32 = 104;
const F_SCALARLESEL: u32 = 336;
const F_SCALARGESEL: u32 = 337;

struct RangeQueryClause<'mcx> {
    var: Node<'mcx>,
    have_lobound: bool,
    have_hibound: bool,
    lobound: f64,
    hibound: f64,
}

pub fn clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    clauselist_selectivity_ext(run, clauses, varrelid, jointype, sjinfo, true)
}

// clauselist_selectivity_ext (clausesel.c): use_extended_stats=false is the
// re-entry form used by the extended-statistics estimators themselves.
pub(crate) fn clauselist_selectivity_ext<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    if clauses.len() == 1 {
        return clause_selectivity_rinfo_ext(
            run,
            clauses[0],
            varrelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    }
    let mut s1 = 1.0;
    let mut estimated: PgVec<'_, bool> = mcx::vec_from_elem_in(run.mcx, false, clauses.len());
    if use_extended_stats {
        if let Some(rel) =
            crate::extended_stats::find_single_rel_for_clauses(run, clauses)
        {
            if run.root.rel(rel).rtekind == types_pathnodes::RTE_RELATION
                && !run.root.rel(rel).statlist.is_empty()
            {
                s1 *= crate::extended_stats::statext_clauselist_selectivity(
                    run, clauses, varrelid, jointype, sjinfo, rel, &mut estimated,
                )?;
            }
        }
    }
    let mut rqlist: PgVec<'mcx, RangeQueryClause<'mcx>> = PgVec::new_in(run.mcx);
    for (i, &rid) in clauses.iter().enumerate() {
        if estimated[i] {
            continue;
        }
        let s2 =
            clause_selectivity_rinfo_ext(run, rid, varrelid, jointype, sjinfo, use_extended_stats)?;
        if run.root.rinfo(rid).pseudoconstant {
            s1 *= s2;
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        merge_clause(run, Some(rid), clause, s2, &mut s1, &mut rqlist)?;
    }
    merge_range_pairs(run, &rqlist, varrelid, &mut s1)?;
    Ok(s1)
}

fn clauselist_selectivity_nodes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    if clauses.len() == 1 {
        return clause_selectivity_node_ext(
            run,
            clauses[0],
            varrelid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    }
    let mut s1 = 1.0;
    let mut rqlist: PgVec<'mcx, RangeQueryClause<'mcx>> = PgVec::new_in(run.mcx);
    for &clause in clauses {
        let s2 =
            clause_selectivity_node_ext(run, clause, varrelid, jointype, sjinfo, use_extended_stats)?;
        merge_clause(run, None, clause, s2, &mut s1, &mut rqlist)?;
    }
    merge_range_pairs(run, &rqlist, varrelid, &mut s1)?;
    Ok(s1)
}

// clauselist_selectivity_or (clausesel.c), bare-node form (C's OR args are
// sub-RestrictInfos on rinfo->orclause; the port keeps them as bare nodes).
pub(crate) fn clauselist_selectivity_or_nodes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[Node<'mcx>],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    let mut s1 = 0.0;
    let mut estimated: PgVec<'_, bool> = mcx::vec_from_elem_in(run.mcx, false, clauses.len());
    if use_extended_stats {
        if let Some(rel) =
            crate::extended_stats::find_single_rel_for_clause_nodes(run, clauses)?
        {
            if run.root.rel(rel).rtekind == types_pathnodes::RTE_RELATION
                && !run.root.rel(rel).statlist.is_empty()
            {
                s1 = crate::extended_stats::statext_clauselist_selectivity_or_nodes(
                    run, clauses, varrelid, jointype, sjinfo, rel, &mut estimated,
                )?;
            }
        }
    }
    for (i, &clause) in clauses.iter().enumerate() {
        if estimated[i] {
            continue;
        }
        let s2 =
            clause_selectivity_node_ext(run, clause, varrelid, jointype, sjinfo, use_extended_stats)?;
        s1 = s1 + s2 - s1 * s2;
    }
    Ok(s1)
}

fn merge_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: Option<RinfoId>,
    clause: Node<'mcx>,
    s2: f64,
    s1: &mut f64,
    rqlist: &mut PgVec<'mcx, RangeQueryClause<'mcx>>,
) -> PgResult<()> {
    if let Some(op) = clause.as_op_expr().filter(|o| o.args.len() == 2) {
        let (arg0, arg1) = (op.args.nth(0), op.args.nth(1));
        let mut varonleft = true;
        let ok = match rid {
            Some(rid) => {
                let right_empty =
                    crate::relnode::relids_is_empty(&run.root.rinfo(rid).right_relids);
                let left_empty =
                    crate::relnode::relids_is_empty(&run.root.rinfo(rid).left_relids);
                run.root.rinfo(rid).num_base_rels == 1
                    && ((right_empty && !clauses::contain_volatile_functions(arg1)?) || {
                        varonleft = false;
                        left_empty && !clauses::contain_volatile_functions(arg0)?
                    })
            }
            None => {
                num_relids_of(run, clause)? == 1
                    && (clauses::is_pseudo_constant_clause(arg1)? || {
                        varonleft = false;
                        clauses::is_pseudo_constant_clause(arg0)?
                    })
            }
        };
        if ok {
            match crate::syscache_memo::get_oprrest(run, op.opno)? {
                F_SCALARLTSEL | F_SCALARLESEL => {
                    add_range_clause(rqlist, clause, varonleft, true, s2)?
                }
                F_SCALARGTSEL | F_SCALARGESEL => {
                    add_range_clause(rqlist, clause, varonleft, false, s2)?
                }
                _ => *s1 *= s2,
            }
            return Ok(());
        }
    }
    *s1 *= s2;
    Ok(())
}

fn add_range_clause<'mcx>(
    rqlist: &mut PgVec<'mcx, RangeQueryClause<'mcx>>,
    clause: Node<'mcx>,
    varonleft: bool,
    is_lt_sel: bool,
    s2: f64,
) -> PgResult<()> {
    let op = clause.as_op_expr().expect("range clause is an OpExpr");
    let (var, is_lobound) = if varonleft {
        (op.args.nth(0), !is_lt_sel)
    } else {
        (op.args.nth(1), is_lt_sel)
    };
    for rq in rqlist.iter_mut() {
        if !equal(var, rq.var) {
            continue;
        }
        if is_lobound {
            if !rq.have_lobound {
                rq.have_lobound = true;
                rq.lobound = s2;
            } else if rq.lobound > s2 {
                rq.lobound = s2;
            }
        } else if !rq.have_hibound {
            rq.have_hibound = true;
            rq.hibound = s2;
        } else if rq.hibound > s2 {
            rq.hibound = s2;
        }
        return Ok(());
    }
    rqlist.push(RangeQueryClause {
        var,
        have_lobound: is_lobound,
        have_hibound: !is_lobound,
        lobound: if is_lobound { s2 } else { 0.0 },
        hibound: if is_lobound { 0.0 } else { s2 },
    });
    Ok(())
}

fn merge_range_pairs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rqlist: &[RangeQueryClause<'mcx>],
    varrelid: i32,
    s1: &mut f64,
) -> PgResult<()> {
    for rq in rqlist {
        if rq.have_lobound && rq.have_hibound {
            // C's exact float-equality default probes.
            let s2 = if rq.hibound == DEFAULT_INEQ_SEL || rq.lobound == DEFAULT_INEQ_SEL {
                DEFAULT_RANGE_INEQ_SEL
            } else {
                let mut s2 = rq.hibound + rq.lobound - 1.0;
                s2 += crate::selfuncs::nulltestsel(run, true, rq.var, varrelid)?;
                if s2 <= 0.0 {
                    s2 = if s2 < -0.01 { DEFAULT_RANGE_INEQ_SEL } else { 1.0e-10 };
                }
                s2
            };
            *s1 *= s2;
        } else if rq.have_lobound {
            *s1 *= rq.lobound;
        } else {
            *s1 *= rq.hibound;
        }
    }
    Ok(())
}

pub fn clause_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    clause_selectivity_rinfo_ext(run, rinfo, varrelid, jointype, sjinfo, true)
}

// clause_selectivity_ext (clausesel.c), RestrictInfo arm.
fn clause_selectivity_rinfo_ext<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    if run.root.rinfo(rinfo).pseudoconstant
        && run.root.expr_node(run.root.rinfo(rinfo).clause).node_tag() != NodeTag::T_Const
    {
        return Ok(1.0);
    }

    let mut cacheable = false;
    {
        let r = run.root.rinfo(rinfo);
        if varrelid == 0
            || r.num_base_rels == 0
            || (r.num_base_rels == 1 && relids_is_member(varrelid, &r.clause_relids))
        {
            if jointype == JOIN_INNER {
                if r.norm_selec >= 0.0 {
                    return Ok(r.norm_selec);
                }
            } else if r.outer_selec >= 0.0 {
                return Ok(r.outer_selec);
            }
            cacheable = true;
        }
    }

    debug_assert!(run.root.rinfo(rinfo).orclause.is_none());
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);

    let s1 = match clause.node_tag() {
        NodeTag::T_OpExpr => {
            let (opno, inputcollid, args): (u32, u32, PgVec<'mcx, NodeId>) = {
                let o = clause.as_op_expr().unwrap();
                let mut ids = PgVec::new_in(run.mcx);
                for a in &o.args {
                    ids.push(run.intern_expr(a));
                }
                (o.opno, o.inputcollid, ids)
            };
            if treat_as_join_clause(run, Some(rinfo), clause, varrelid, sjinfo)? {
                crate::plancat::join_selectivity(run, opno, &args, inputcollid, jointype, sjinfo)?
            } else {
                crate::plancat::restriction_selectivity(run, opno, &args, inputcollid, varrelid)?
            }
        }
        _ => clause_selectivity_node_ext(run, clause, varrelid, jointype, sjinfo, use_extended_stats)?,
    };

    if cacheable {
        if jointype == JOIN_INNER {
            run.root.rinfo_mut(rinfo).norm_selec = s1;
        } else {
            run.root.rinfo_mut(rinfo).outer_selec = s1;
        }
    }
    Ok(s1)
}

pub(crate) fn clause_selectivity_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    clause_selectivity_node_ext(run, clause, varrelid, jointype, sjinfo, true)
}

// clause_selectivity_ext (clausesel.c), bare-node arms.
pub(crate) fn clause_selectivity_node_ext<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    match clause.node_tag() {
        NodeTag::T_Var => {
            let v = clause.as_var().unwrap();
            if v.varlevelsup == 0 && (varrelid == 0 || varrelid == v.varno) {
                crate::selfuncs::boolvarsel(run, clause, varrelid)
            } else {
                // C: uplevel or other-rel Var takes the default.
                Ok(0.5)
            }
        }
        NodeTag::T_Const => {
            let c = clause.as_const().unwrap();
            Ok(if c.constisnull || !c.constvalue.as_bool() { 0.0 } else { 1.0 })
        }
        NodeTag::T_RelabelType => clause_selectivity_node_ext(
            run,
            clause.as_relabel_type().unwrap().arg,
            varrelid,
            jointype,
            sjinfo,
            use_extended_stats,
        ),
        NodeTag::T_BoolExpr => {
            use types_nodes::primnodes::BoolExprType;
            let b = clause.as_bool_expr().unwrap();
            match b.boolop {
                BoolExprType::NOT_EXPR => Ok(1.0
                    - clause_selectivity_node_ext(
                        run,
                        b.args.nth(0),
                        varrelid,
                        jointype,
                        sjinfo,
                        use_extended_stats,
                    )?),
                BoolExprType::AND_EXPR => clauselist_selectivity_nodes(
                    run,
                    b.args.as_slice(),
                    varrelid,
                    jointype,
                    sjinfo,
                    use_extended_stats,
                ),
                BoolExprType::OR_EXPR => clauselist_selectivity_or_nodes(
                    run,
                    b.args.as_slice(),
                    varrelid,
                    jointype,
                    sjinfo,
                    use_extended_stats,
                ),
            }
        }
        NodeTag::T_OpExpr => {
            let (opno, inputcollid, args): (u32, u32, PgVec<'mcx, NodeId>) = {
                let o = clause.as_op_expr().unwrap();
                let mut ids = PgVec::new_in(run.mcx);
                for a in &o.args {
                    ids.push(run.intern_expr(a));
                }
                (o.opno, o.inputcollid, ids)
            };
            let s = if treat_as_join_clause(run, None, clause, varrelid, sjinfo)? {
                crate::plancat::join_selectivity(run, opno, &args, inputcollid, jointype, sjinfo)?
            } else {
                crate::plancat::restriction_selectivity(run, opno, &args, inputcollid, varrelid)?
            };
            Ok(s)
        }
        NodeTag::T_FuncExpr => {
            let f = clause.as_func_expr().unwrap();
            let mut ids = PgVec::new_in(run.mcx);
            for a in &f.args {
                ids.push(run.intern_expr(a));
            }
            let is_join = treat_as_join_clause(run, None, clause, varrelid, sjinfo)?;
            crate::plancat::function_selectivity(
                run,
                f.funcid,
                &ids,
                f.inputcollid,
                is_join,
                varrelid,
                jointype,
                sjinfo,
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let is_join = treat_as_join_clause(run, None, clause, varrelid, sjinfo)?;
            crate::selfuncs::scalararraysel(run, clause, is_join, varrelid, jointype, sjinfo)
        }
        NodeTag::T_NullTest => {
            use types_nodes::primnodes::NullTestType;
            let nt = clause.as_null_test().unwrap();
            crate::selfuncs::nulltestsel(
                run,
                nt.nulltesttype == NullTestType::IS_NULL,
                nt.arg.expect("NullTest arg"),
                varrelid,
            )
        }
        // C: "can we do better?" — DistinctExpr is a fixed 0.5.
        NodeTag::T_DistinctExpr => Ok(0.5),
        NodeTag::T_BooleanTest => {
            let bt = clause.as_boolean_test().unwrap();
            crate::selfuncs::booltestsel(
                run,
                bt.booltesttype,
                bt.arg.expect("BooleanTest.arg"),
                varrelid,
                jointype,
                sjinfo,
            )
        }
        // CURRENT OF selects at most one row of its table.
        NodeTag::T_CurrentOfExpr => {
            let cvarno = clause.as_current_of_expr().unwrap().cvarno;
            let crel_id = crate::relnode::find_base_rel(&run.root, cvarno as i32);
            let tuples = run.root.rel(crel_id).tuples;
            Ok(if tuples > 0.0 { 1.0 / tuples } else { 0.5 })
        }
        NodeTag::T_RowCompareExpr => rowcomparesel(run, clause, varrelid, jointype, sjinfo),
        // C's catch-all default: no way to estimate, use 0.5.
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_Param => Ok(0.5),
        // C's final else: boolvarsel. NullIfExpr belongs here (GL-TESTFIX-1
        // / GL-TESTRIGS-1 F-R3-1): its C node REPRESENTATION is OpExpr, but
        // its tag is T_NullIfExpr, so C's `is_opclause(clause) ||
        // IsA(clause, DistinctExpr)` arm rejects it and clausesel.c's final
        // else takes it — a boolean NULLIF qual estimates via boolvarsel,
        // never via the operator's restriction estimator.
        NodeTag::T_CaseExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_JsonIsPredicate
        | NodeTag::T_NullIfExpr
        | NodeTag::T_PlaceHolderVar => {
            crate::selfuncs::boolvarsel(run, clause, varrelid)
        }
        other => panic!("clause_selectivity_ext (clausesel.c): {other:?}; M2 qual lane"),
    }
}


// rowcomparesel (selfuncs.c): estimate on the leading column pair only.
fn rowcomparesel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    let rc = clause.as_row_compare_expr().unwrap();
    let opno = rc.opnos.nth(0);
    let inputcollid = rc.inputcollids.nth(0);
    let (larg, rarg) = (rc.largs.nth(0), rc.rargs.nth(0));
    let args = {
        let mut ids = PgVec::new_in(run.mcx);
        ids.push(run.intern_expr(larg));
        ids.push(run.intern_expr(rarg));
        ids
    };
    let is_join = if varrelid != 0 || sjinfo.is_none() {
        false
    } else {
        let mut bms = vars::pull_varnos(run.mcx, larg)?;
        bms.add_members(run.mcx, &vars::pull_varnos(run.mcx, rarg)?)?;
        debug_assert!(crate::relnode::relids_is_unset(&run.root.outer_join_rels));
        bms.iter().count() > 1
    };
    if is_join {
        crate::plancat::join_selectivity(run, opno, &args, inputcollid, jointype, sjinfo)
    } else {
        crate::plancat::restriction_selectivity(run, opno, &args, inputcollid, varrelid)
    }
}

// treat_as_join_clause (clausesel.c).
fn treat_as_join_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: Option<RinfoId>,
    clause: Node<'mcx>,
    varrelid: i32,
    sjinfo: Option<&SpecialJoinInfo<'_>>,
) -> PgResult<bool> {
    if varrelid != 0 || sjinfo.is_none() {
        return Ok(false);
    }
    match rinfo {
        Some(r) => Ok(run.root.rinfo(r).num_base_rels > 1),
        None => Ok(num_relids_of(run, clause)? > 1),
    }
}

// NumRelids (clauses.c): baserel count only — outer-join relids (pulled from
// varnullingrels) are deleted, as C's bms_del_members.
fn num_relids_of<'mcx>(run: &mut PlannerRun<'mcx>, clause: Node<'mcx>) -> PgResult<i32> {
    let bms = vars::pull_varnos(run.mcx, clause)?;
    Ok(bms
        .iter()
        .filter(|&r| !relids_is_member(r, &run.root.outer_join_rels))
        .count() as i32)
}
