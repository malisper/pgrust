//! clausesel.c slice: single-RestrictInfo clauselist_selectivity with the
//! norm_selec cache. Multi-clause combination (extended stats, range pairing)
//! is a named panic.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::NodeTag;
use types_pathnodes::{JoinType, NodeId, RinfoId, SpecialJoinInfo, JOIN_INNER};

use crate::relnode::relids_is_member;
use crate::run::PlannerRun;

pub fn clauselist_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[RinfoId],
    varrelid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    match clauses {
        [] => Ok(1.0),
        [one] => clause_selectivity(run, *one, varrelid, jointype, sjinfo),
        _ => panic!(
            "clauselist_selectivity_ext (clausesel.c): {} clauses; M2 multi-qual lane",
            clauses.len()
        ),
    }
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

    if run.root.rinfo(rinfo).orclause.is_some() {
        panic!("clause_selectivity_ext (clausesel.c): OR clause; M2 OR lane");
    }
    let clause_id = run.root.rinfo(rinfo).clause;
    let clause = *run.root.expr_node(clause_id);

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
            if treat_as_join_clause(run, rinfo, varrelid, sjinfo) {
                panic!("join_selectivity (plancat.c): M2 join lane");
            }
            crate::plancat::restriction_selectivity(run, opno, &args, inputcollid, varrelid)?
        }
        other => panic!("clause_selectivity_ext (clausesel.c): {other:?}; M2 qual lane"),
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

// treat_as_join_clause (clausesel.c), rinfo-bearing arm.
fn treat_as_join_clause(
    run: &PlannerRun<'_>,
    rinfo: RinfoId,
    varrelid: i32,
    sjinfo: Option<&SpecialJoinInfo<'_>>,
) -> bool {
    if varrelid != 0 {
        false
    } else if sjinfo.is_some() {
        true
    } else {
        run.root.rinfo(rinfo).num_base_rels > 1
    }
}
