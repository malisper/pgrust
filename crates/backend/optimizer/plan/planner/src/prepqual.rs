//! prepqual.c slice: canonicalize_qual (find_duplicate_ors and the inverse OR
//! distributive law). negate_clause lives in the clauses crate's fold module.

use clauses::{is_andclause, is_orclause, make_andclause, make_bool_const, make_orclause};
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::{equal, Node, NodeList, NodeTag};

pub fn canonicalize_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: Node<'mcx>,
    is_check: bool,
) -> PgResult<Node<'mcx>> {
    debug_assert!(qual.node_tag() != NodeTag::T_List);
    find_duplicate_ors(mcx, qual, is_check)
}

fn pull_ands<'mcx>(
    mcx: Mcx<'mcx>,
    andlist: &NodeList<'mcx>,
    out: &mut NodeList<'mcx>,
) -> PgResult<()> {
    for subexpr in andlist {
        if is_andclause(subexpr) {
            pull_ands(mcx, &subexpr.as_bool_expr().unwrap().args, out)?;
        } else {
            out.lappend(mcx, subexpr)?;
        }
    }
    Ok(())
}

fn pull_ors<'mcx>(
    mcx: Mcx<'mcx>,
    orlist: &NodeList<'mcx>,
    out: &mut NodeList<'mcx>,
) -> PgResult<()> {
    for subexpr in orlist {
        if is_orclause(subexpr) {
            pull_ors(mcx, &subexpr.as_bool_expr().unwrap().args, out)?;
        } else {
            out.lappend(mcx, subexpr)?;
        }
    }
    Ok(())
}

// find_duplicate_ors: NULL consts fold as FALSE in WHERE, TRUE in CHECK
// (valid only at these top levels).
fn find_duplicate_ors<'mcx>(
    mcx: Mcx<'mcx>,
    qual: Node<'mcx>,
    is_check: bool,
) -> PgResult<Node<'mcx>> {
    if is_orclause(qual) {
        let mut orlist = NodeList::nil();
        for arg in &qual.as_bool_expr().unwrap().args {
            let arg = find_duplicate_ors(mcx, arg, is_check)?;
            if let Some(carg) = arg.as_const() {
                if is_check {
                    if !carg.constisnull && !carg.constvalue.as_bool() {
                        continue;
                    }
                    return make_bool_const(mcx, true, false);
                }
                if carg.constisnull || !carg.constvalue.as_bool() {
                    continue;
                }
                return Ok(arg);
            }
            orlist.lappend(mcx, arg)?;
        }
        let mut flat = NodeList::nil();
        pull_ors(mcx, &orlist, &mut flat)?;
        process_duplicate_ors(mcx, &flat)
    } else if is_andclause(qual) {
        let mut andlist = NodeList::nil();
        for arg in &qual.as_bool_expr().unwrap().args {
            let arg = find_duplicate_ors(mcx, arg, is_check)?;
            if let Some(carg) = arg.as_const() {
                if is_check {
                    if carg.constisnull || carg.constvalue.as_bool() {
                        continue;
                    }
                    return Ok(arg);
                }
                if !carg.constisnull && carg.constvalue.as_bool() {
                    continue;
                }
                return make_bool_const(mcx, false, false);
            }
            andlist.lappend(mcx, arg)?;
        }
        let mut flat = NodeList::nil();
        pull_ands(mcx, &andlist, &mut flat)?;
        match flat.len() {
            0 => make_bool_const(mcx, true, false),
            1 => Ok(flat.nth(0)),
            _ => make_andclause(mcx, flat),
        }
    } else {
        Ok(qual)
    }
}

fn list_member(list: &NodeList<'_>, node: Node<'_>) -> bool {
    list.iter().any(|n| equal(n, node))
}

// process_duplicate_ors (prepqual.c): ((A AND B) OR (A AND C)) becomes
// (A AND (B OR C)).
fn process_duplicate_ors<'mcx>(
    mcx: Mcx<'mcx>,
    orlist: &NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    if orlist.is_nil() {
        return make_bool_const(mcx, false, false);
    }
    if orlist.len() == 1 {
        return Ok(orlist.nth(0));
    }

    let mut reference: NodeList<'mcx> = NodeList::nil();
    let mut num_subclauses = 0usize;
    for clause in orlist {
        if is_andclause(clause) {
            let subclauses = &clause.as_bool_expr().unwrap().args;
            if reference.is_nil() || subclauses.len() < num_subclauses {
                reference = subclauses.clone_in(mcx)?;
                num_subclauses = subclauses.len();
            }
        } else {
            reference = NodeList::make1(mcx, clause)?;
            break;
        }
    }

    // list_union(NIL, reference): dedupe by equal().
    let mut uniq = NodeList::nil();
    for c in &reference {
        if !list_member(&uniq, c) {
            uniq.lappend(mcx, c)?;
        }
    }

    let mut winners = NodeList::nil();
    for refclause in &uniq {
        let win = orlist.iter().all(|clause| {
            if is_andclause(clause) {
                list_member(&clause.as_bool_expr().unwrap().args, refclause)
            } else {
                equal(refclause, clause)
            }
        });
        if win {
            winners.lappend(mcx, refclause)?;
        }
    }

    if winners.is_nil() {
        return make_orclause(mcx, orlist.clone_in(mcx)?);
    }

    let mut neworlist = NodeList::nil();
    let mut degenerate = false;
    for clause in orlist {
        if is_andclause(clause) {
            // list_difference: drop every subclause equal() to a winner.
            let mut subclauses = NodeList::nil();
            for sc in &clause.as_bool_expr().unwrap().args {
                if !list_member(&winners, sc) {
                    subclauses.lappend(mcx, sc)?;
                }
            }
            match subclauses.len() {
                0 => {
                    degenerate = true;
                    break;
                }
                1 => neworlist.lappend(mcx, subclauses.nth(0))?,
                _ => neworlist.lappend(mcx, make_andclause(mcx, subclauses)?)?,
            }
        } else if !list_member(&winners, clause) {
            neworlist.lappend(mcx, clause)?;
        } else {
            degenerate = true;
            break;
        }
    }

    if !degenerate && !neworlist.is_nil() {
        if neworlist.len() == 1 {
            winners.lappend(mcx, neworlist.nth(0))?;
        } else {
            let mut flat = NodeList::nil();
            pull_ors(mcx, &neworlist, &mut flat)?;
            winners.lappend(mcx, make_orclause(mcx, flat)?)?;
        }
    }

    if winners.len() == 1 {
        Ok(winners.nth(0))
    } else {
        let mut flat = NodeList::nil();
        pull_ands(mcx, &winners, &mut flat)?;
        make_andclause(mcx, flat)
    }
}
