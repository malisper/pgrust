//! orclauses.c: extract single-relation restriction OR clauses from join OR
//! clauses.

use types_error::PgResult;
use types_nodes::{Node, NodeList};
use types_pathnodes::{RelId, RinfoId, JOIN_INNER, RELOPT_BASEREL};

use crate::joinrels::init_dummy_sjinfo;
use crate::relnode::{relids_copy, relids_difference, relids_equal};
use crate::run::PlannerRun;

pub fn extract_restriction_or_clauses<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else { continue };
        debug_assert_eq!(run.root.rel(rel).relid as usize, rti);
        if run.root.rel(rel).reloptkind != RELOPT_BASEREL {
            continue;
        }
        // Any joinclause movable here by the parameterized-path rules is fair
        // game, even though this is not a parameterized path.
        let joininfo = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).joininfo);
        for &rid in joininfo.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            if clauses::is_orclause(clause)
                && crate::indxpath::join_clause_is_movable_to(run, rid, rel)
            {
                if let Some(orclause) = extract_or_clause(run, rid, rel)? {
                    consider_new_or_clause(run, rel, orclause, rid)?;
                }
            }
        }
    }
    Ok(())
}

fn is_safe_restriction_clause_for(run: &PlannerRun<'_>, rid: RinfoId, rel: RelId) -> PgResult<bool> {
    let ri = run.root.rinfo(rid);
    if ri.pseudoconstant {
        return Ok(false);
    }
    if !relids_equal(&ri.clause_relids, &run.root.rel(rel).relids) {
        return Ok(false);
    }
    // No extra evaluations of volatile functions.
    if clauses::contain_volatile_functions(*run.root.expr_node(ri.clause))? {
        return Ok(false);
    }
    Ok(true)
}

// extract_or_clause (orclauses.c). C descends orclause's embedded
// sub-RestrictInfos; orclause stays None repo-wide, so or_arm_rinfo
// synthesizes each arm's rinfo on the way down (tidpath.rs precedent).
fn extract_or_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    or_rid: RinfoId,
    rel: RelId,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    let mut clauselist: NodeList<'mcx> = NodeList::nil();
    let clause = *run.root.expr_node(run.root.rinfo(or_rid).clause);
    debug_assert!(clauses::is_orclause(clause));
    let mut orargs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    orargs.extend(clause.as_bool_expr().expect("OR clause").args.iter());

    for &orarg in orargs.iter() {
        let mut subclauses: NodeList<'mcx> = NodeList::nil();
        if clauses::is_andclause(orarg) {
            let mut andargs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
            andargs.extend(orarg.as_bool_expr().expect("AND clause").args.iter());
            for &a in andargs.iter() {
                let arid = crate::indxpath::or_arm_rinfo(run, or_rid, a)?;
                if clauses::is_orclause(a) {
                    // Nested OR: must recurse to strip/rebuild all the way down.
                    if let Some(sub) = extract_or_clause(run, arid, rel)? {
                        subclauses.lappend(mcx, sub)?;
                    }
                } else if is_safe_restriction_clause_for(run, arid, rel)? {
                    subclauses.lappend(mcx, a)?;
                }
            }
        } else {
            debug_assert!(!clauses::is_orclause(orarg), "unflattened OR");
            let arid = crate::indxpath::or_arm_rinfo(run, or_rid, orarg)?;
            if is_safe_restriction_clause_for(run, arid, rel)? {
                subclauses.lappend(mcx, orarg)?;
            }
        }

        // Every arm must yield something, else the whole OR is unusable.
        if subclauses.is_nil() {
            return Ok(None);
        }

        // Preserve AND/OR flatness: a lone OR subclause contributes its args.
        let subclause = clauses::make_ands_explicit(mcx, &subclauses)?;
        if clauses::is_orclause(subclause) {
            clauselist.concat(mcx, &subclause.as_bool_expr().expect("OR clause").args)?;
        } else {
            clauselist.lappend(mcx, subclause)?;
        }
    }

    if !clauselist.is_nil() {
        Ok(Some(clauses::make_orclause(mcx, clauselist)?))
    } else {
        Ok(None)
    }
}

fn consider_new_or_clause<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    orclause: Node<'mcx>,
    join_or_rid: RinfoId,
) -> PgResult<()> {
    let mcx = run.mcx;
    let security_level = run.root.rinfo(join_or_rid).security_level;
    let or_rid = crate::initsplan::make_restrictinfo(
        run,
        orclause,
        true,
        false,
        false,
        false,
        security_level,
        crate::relnode::relids_empty(),
        crate::relnode::relids_empty(),
        crate::relnode::relids_empty(),
    )?;
    let or_selec = crate::clausesel::clause_selectivity(run, or_rid, 0, JOIN_INNER, None)?;
    // Below 0.9 selectivity the extracted qual rejects enough rows to beat
    // its duplicate-evaluation cost (C's threshold).
    if or_selec > 0.9 {
        return Ok(());
    }
    run.root.rel_mut(rel).baserestrictinfo.push(or_rid);
    let minsec = run.root.rel(rel).baserestrict_min_security.min(security_level);
    run.root.rel_mut(rel).baserestrict_min_security = minsec;

    // Compensate the join OR clause's cached inner-join selectivity so the
    // joinrel size estimate is unchanged by the redundant lower qual; relies
    // on norm_selec staying cached (C's "major hack", verbatim).
    if or_selec > 0.0 {
        let left = relids_difference(
            mcx,
            &run.root.rinfo(join_or_rid).clause_relids,
            &run.root.rel(rel).relids,
        );
        let right = relids_copy(mcx, &run.root.rel(rel).relids);
        let sjinfo = init_dummy_sjinfo(run, left, right);
        let orig_selec =
            crate::clausesel::clause_selectivity(run, join_or_rid, 0, JOIN_INNER, Some(&sjinfo))?;
        run.root.rinfo_mut(join_or_rid).norm_selec = (orig_selec / or_selec).min(1.0);
    }
    Ok(())
}
