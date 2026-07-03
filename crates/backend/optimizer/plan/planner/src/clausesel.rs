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
    if clauses.len() == 1 {
        return clause_selectivity(run, clauses[0], varrelid, jointype, sjinfo);
    }
    let mut s1 = 1.0;
    let mut rqlist: PgVec<'mcx, RangeQueryClause<'mcx>> = PgVec::new_in(run.mcx);
    for &rid in clauses {
        let s2 = clause_selectivity(run, rid, varrelid, jointype, sjinfo)?;
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
) -> PgResult<f64> {
    if clauses.len() == 1 {
        return clause_selectivity_node(run, clauses[0], varrelid, jointype, sjinfo);
    }
    let mut s1 = 1.0;
    let mut rqlist: PgVec<'mcx, RangeQueryClause<'mcx>> = PgVec::new_in(run.mcx);
    for &clause in clauses {
        let s2 = clause_selectivity_node(run, clause, varrelid, jointype, sjinfo)?;
        merge_clause(run, None, clause, s2, &mut s1, &mut rqlist)?;
    }
    merge_range_pairs(run, &rqlist, varrelid, &mut s1)?;
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
            match lsyscache::get_oprrest(op.opno)? {
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
        _ => clause_selectivity_node(run, clause, varrelid, jointype, sjinfo)?,
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

// clause_selectivity_ext (clausesel.c), bare-node arms.
fn clause_selectivity_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    match clause.node_tag() {
        NodeTag::T_Const => {
            let c = clause.as_const().unwrap();
            Ok(if c.constisnull || !c.constvalue.as_bool() { 0.0 } else { 1.0 })
        }
        NodeTag::T_BoolExpr => {
            use types_nodes::primnodes::BoolExprType;
            let b = clause.as_bool_expr().unwrap();
            match b.boolop {
                BoolExprType::NOT_EXPR => Ok(1.0
                    - clause_selectivity_node(run, b.args.nth(0), varrelid, jointype, sjinfo)?),
                BoolExprType::AND_EXPR => clauselist_selectivity_nodes(
                    run,
                    b.args.as_slice(),
                    varrelid,
                    jointype,
                    sjinfo,
                ),
                BoolExprType::OR_EXPR => {
                    let mut s1 = 0.0;
                    for arg in &b.args {
                        let s2 =
                            clause_selectivity_node(run, arg, varrelid, jointype, sjinfo)?;
                        s1 = s1 + s2 - s1 * s2;
                    }
                    Ok(s1)
                }
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
        // C's catch-all default: no way to estimate, use 0.5.
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_Param => Ok(0.5),
        other => panic!("clause_selectivity_ext (clausesel.c): {other:?}; M2 qual lane"),
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

// NumRelids (clauses.c); no outer-join relids on this lane.
fn num_relids_of<'mcx>(run: &mut PlannerRun<'mcx>, clause: Node<'mcx>) -> PgResult<i32> {
    debug_assert!(run.root.outer_join_rels.is_none());
    let bms = vars::pull_varnos(run.mcx, clause)?;
    Ok(bms.iter().count() as i32)
}
