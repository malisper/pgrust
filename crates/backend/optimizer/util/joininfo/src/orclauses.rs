//! `optimizer/plan/orclauses.c` — extract restriction OR clauses from join OR
//! clauses.

use alloc::vec::Vec;

use ::nodes_core::makefuncs::{make_ands_explicit, make_orclause};
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::primnodes::Expr;
use ::types_core::primitive::Index;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerInfo, RelId, RinfoId, JOIN_INNER};

use crate::bms;
use crate::restrictinfo::{
    join_clause_is_movable_to, make_restrictinfo, restriction_is_or_clause,
};
use path_small_seams as small_seam;

use ::nodes::primnodes::{AND_EXPR, OR_EXPR};

#[inline]
fn is_orclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == OR_EXPR)
}
#[inline]
fn is_andclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == AND_EXPR)
}

const RELOPT_BASEREL: ::pathnodes::RelOptKind = ::pathnodes::RELOPT_BASEREL;

/// `extract_restriction_or_clauses`
///	  Examine join OR-of-AND clauses to see if any useful restriction OR clauses
///	  can be extracted.  If so, add them to the query.
pub fn extract_restriction_or_clauses<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo) -> PgResult<()> {
    // Examine each baserel for potential join OR clauses.
    for rti in 1..root.simple_rel_array_size {
        let rel = match root.simple_rel_array.get(rti as usize).copied().flatten() {
            // there may be empty slots corresponding to non-baserel RTEs
            Some(r) => r,
            None => continue,
        };

        debug_assert!(root.rel(rel).relid == rti as Index); // sanity check on array

        // ignore RTEs that are "other rels"
        if root.rel(rel).reloptkind != RELOPT_BASEREL {
            continue;
        }

        // Find potentially interesting OR joinclauses.  We can use any joinclause
        // considered safe to move to this rel by the parameterized-path machinery.
        let joininfo = root.rel(rel).joininfo.clone();
        for rinfo in joininfo {
            if restriction_is_or_clause(root, rinfo) && join_clause_is_movable_to(root, rinfo, rel) {
                // Try to extract a qual for this rel only.
                if let Some(orclause) = extract_or_clause(run.mcx(), root, rinfo, rel)? {
                    // If successful, decide whether we want to use the clause, and
                    // insert it into the rel's restrictinfo list if so.
                    consider_new_or_clause(run, root, rel, orclause, rinfo)?;
                }
            }
        }
    }
    Ok(())
}

/// Is the given primitive (non-OR) RestrictInfo safe to move to the rel?
fn is_safe_restriction_clause_for(root: &PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    let ri = root.rinfo(rinfo);
    // We want clauses that mention the rel, and only the rel.  Pseudoconstant
    // clauses can be rejected quickly.
    if ri.pseudoconstant {
        return false;
    }
    if !bms::relids_equal::call(&ri.clause_relids, &root.rel(rel).relids) {
        return false;
    }
    // We don't want extra evaluations of any volatile functions.
    let clause = root.node(ri.clause);
    if small_seam::contain_volatile_functions_expr::call(clause) {
        return false;
    }
    true
}

/// Try to extract a restriction clause mentioning only "rel" from the given join
/// OR-clause. Returns an OR clause (not a RestrictInfo!) pertaining to rel, or
/// `None` if no OR clause could be extracted.
fn extract_or_clause<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    or_rinfo: RinfoId,
    rel: RelId,
) -> PgResult<Option<Expr<'mcx>>> {
    let mut clauselist: Vec<Expr<'mcx>> = Vec::new();

    // Scan each arm of the input OR clause.  We descend into or_rinfo->orclause,
    // which has RestrictInfo nodes embedded below the toplevel OR/AND structure.
    // The arm clauses are moved into the freshly built result OR/AND, so every
    // owned copy goes through `Expr::clone_in` (the derived `Expr::clone` panics
    // on a context-allocated child); the read-only structural inspection borrows.
    let orclause_id = root
        .rinfo(or_rinfo)
        .orclause
        .expect("extract_or_clause: or_rinfo has no orclause");
    let orclause: &Expr = root.node(orclause_id);
    debug_assert!(is_orclause(orclause));
    let or_args: &[Expr] = match orclause {
        Expr::BoolExpr(b) => &b.args,
        _ => unreachable!(),
    };

    for orarg in or_args {
        let mut subclauses: Vec<Expr<'mcx>> = Vec::new();

        // OR arguments should be ANDs or sub-RestrictInfos.
        if is_andclause(orarg) {
            let andargs: &[Expr] = match orarg {
                Expr::BoolExpr(b) => &b.args,
                _ => unreachable!(),
            };
            for andarg in andargs {
                let rinfo: RinfoId = match andarg {
                    Expr::RestrictInfo(r) => (*r).into(),
                    _ => panic!("extract_or_clause: AND arg is not a RestrictInfo"),
                };
                if restriction_is_or_clause(root, rinfo) {
                    // Recurse to deal with nested OR.  We *must* recurse to find
                    // and strip all RestrictInfos in the expression.
                    if let Some(suborclause) = extract_or_clause(mcx, root, rinfo, rel)? {
                        subclauses.push(suborclause);
                    }
                } else if is_safe_restriction_clause_for(root, rinfo, rel) {
                    subclauses.push(root.node(root.rinfo(rinfo).clause).clone_in(mcx)?);
                }
            }
        } else {
            let rinfo: RinfoId = match orarg {
                Expr::RestrictInfo(r) => (*r).into(),
                _ => panic!("extract_or_clause: OR arg is not a RestrictInfo"),
            };
            debug_assert!(!restriction_is_or_clause(root, rinfo));
            if is_safe_restriction_clause_for(root, rinfo, rel) {
                subclauses.push(root.node(root.rinfo(rinfo).clause).clone_in(mcx)?);
            }
        }

        // If nothing could be extracted from this arm, we can't do anything with
        // this OR clause.
        if subclauses.is_empty() {
            return Ok(None);
        }

        // Add subclause(s) to the result OR.  If more than one, need an AND node.
        // If only one and it is itself an OR node, add its subclauses to the
        // result instead, to preserve AND/OR flatness.
        let subclause = make_ands_explicit(subclauses);
        if is_orclause(&subclause) {
            match subclause {
                Expr::BoolExpr(b) => clauselist.extend(b.args),
                _ => unreachable!(),
            }
        } else {
            clauselist.push(subclause);
        }
    }

    // If we got a restriction clause from every arm, wrap them up in an OR node.
    if !clauselist.is_empty() {
        return Ok(Some(make_orclause(clauselist)));
    }
    Ok(None)
}

/// Consider whether a successfully-extracted restriction OR clause is actually
/// worth using.  If so, add it to the planner's data structures, and adjust the
/// original join clause (join_or_rinfo) to compensate.
fn consider_new_or_clause<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    orclause: Expr<'mcx>,
    join_or_rinfo: RinfoId,
) -> PgResult<()> {
    // Build a RestrictInfo from the new OR clause.  We can assume it's valid as a
    // base restriction clause.
    let security_level = root.rinfo(join_or_rinfo).security_level;
    let or_rinfo = make_restrictinfo(
        run.mcx(),
        root,
        orclause,
        true,
        false,
        false,
        false,
        security_level,
        None,
        None,
        None,
    )?;

    // Estimate its selectivity.  Doing it on the RestrictInfo representation
    // allows the result to get cached, saving work later.
    let or_selec =
        small_seam::clause_selectivity::call(run, root, or_rinfo, 0, JOIN_INNER, None)?;

    // The clause is only worth adding if it rejects a useful fraction of the
    // base relation's rows; threshold 0.9.
    if or_selec > 0.9 {
        return Ok(()); // forget it
    }

    // Add it to the rel's restriction-clause list.
    root.rel_mut(rel).baserestrictinfo.push(or_rinfo);
    let or_sec = root.rinfo(or_rinfo).security_level;
    let cur_min = root.rel(rel).baserestrict_min_security;
    root.rel_mut(rel).baserestrict_min_security = cur_min.min(or_sec);

    // Adjust the original join OR clause's cached selectivity to compensate for
    // the selectivity of the added (but redundant) lower-level qual.
    if or_selec > 0.0 {
        // Make up a SpecialJoinInfo for JOIN_INNER semantics (compare
        // approx_tuple_count() in costsize.c):
        //   init_dummy_sjinfo(&sjinfo,
        //                     bms_difference(join_or_rinfo->clause_relids, rel->relids),
        //                     rel->relids);
        let join_clause_relids = root.rinfo(join_or_rinfo).clause_relids.clone();
        let rel_relids = root.rel(rel).relids.clone();
        let left_relids = bms::relids_difference::call(&join_clause_relids, &rel_relids);
        let sjinfo = init_dummy_sjinfo(left_relids, rel_relids);

        // Compute inner-join size.
        let orig_selec = small_seam::clause_selectivity::call(
            run,
            root,
            join_or_rinfo,
            0,
            JOIN_INNER,
            Some(&sjinfo),
        )?;

        // Hack cached selectivity so join size remains the same.
        let mut norm = orig_selec / or_selec;
        // ensure result stays in sane range
        if norm > 1.0 {
            norm = 1.0;
        }
        root.rinfo_mut(join_or_rinfo).norm_selec = norm;
        // as explained above, we don't touch outer_selec
    }

    Ok(())
}

/// `init_dummy_sjinfo(sjinfo, left_relids, right_relids)` (joinrels.c) — build a
/// JOIN_INNER `SpecialJoinInfo` carrying just the joined-relation info.
///
/// Ported in-crate (rather than seamed) because the joinrels.c seam keys on two
/// `RelId`s, but the orclauses.c caller passes arbitrary relid *sets* (the
/// `bms_difference` outer side is not a single rel). The body is a pure struct
/// initialiser, identical to the joinrels.c original.
fn init_dummy_sjinfo(
    left_relids: ::pathnodes::Relids,
    right_relids: ::pathnodes::Relids,
) -> ::pathnodes::SpecialJoinInfo {
    ::pathnodes::SpecialJoinInfo {
        min_lefthand: bms::relids_copy::call(&left_relids),
        min_righthand: bms::relids_copy::call(&right_relids),
        syn_lefthand: left_relids,
        syn_righthand: right_relids,
        jointype: JOIN_INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        // we don't bother trying to make the remaining fields valid
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    }
}
