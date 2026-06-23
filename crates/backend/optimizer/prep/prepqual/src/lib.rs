//! `backend/optimizer/prep/prepqual.c` — routines for preprocessing
//! qualification expressions.
//!
//! 1:1 port of PostgreSQL 18.3 `prepqual.c` over this repo's lifetime-free
//! owned [`Expr`](::nodes::primnodes::Expr) tree (the same model
//! `clauses.c` is ported onto — `negate_clause` is the `BoolExpr NOT_EXPR`
//! arm's helper there). Lists are `Vec<Expr>`; node builders come from
//! [`nodes_core::makefuncs`] (`makefuncs.c`), never redefined here.
//!
//! prepqual is self-contained boolean canonicalization. Its only genuine
//! externals are the `lsyscache` operator-negator lookup `get_negator`
//! (used by [`negate_clause`]) and the central node `equal()` dispatch
//! (`equalfuncs.c`'s `equal_expr`, used by the generic `list_*` set
//! operations in [`process_duplicate_ors`]). Both go through their owners'
//! seam crates.
//!
//! This unit is `backend-optimizer-prep-core`'s `prepqual.c` member. The four
//! sibling files (prepjointree/preptlist/prepunion/prepagg) walk
//! `PlannerInfo->parse` (the owned `Query`), which is still an opaque
//! `QueryId` handle with no resolver, so they are deferred to a later wave
//! (after the `QueryId -> Query` keystone). prepqual.c references neither
//! `PlannerInfo` nor `Query`, so it lands now.

#![no_std]
// `negate_clause` / `find_duplicate_ors` mirror C's `switch (nodeTag(node))` /
// `if (is_orclause(qual)) ... else if (is_andclause(qual)) ...` chain verbatim;
// collapsing the inner `if`s would obscure the 1:1 reading.
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
// The project-wide error contract is the un-boxed `PgResult` (`Result<_,
// PgError>`); matching the sibling crates' signatures means accepting the
// large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use nodes_core::makefuncs::{
    make_andclause, make_bool_const, make_notclause, make_orclause,
};
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use ::nodes::primnodes::{
    BoolTestType, BooleanTest, Expr, NullTest, NullTestType, AND_EXPR, NOT_EXPR, OR_EXPR,
};

/// The repo's canonical by-value `Datum` (a plan-node `Const` holds a
/// `Datum<'static>`), matching the alias `clauses.c` uses.
type CDatum = types_tuple::heaptuple::Datum<'static>;

/// `DatumGetBool(d)` — `((bool) ((d) & 1))` (matches `clauses.c`).
#[inline]
fn datum_get_bool(d: &types_tuple::heaptuple::Datum<'_>) -> bool {
    (d.as_usize() & 1) != 0
}

/// `get_negator(opno)` via the genuinely-external `lsyscache` seam.
#[inline]
fn get_negator(opno: Oid) -> PgResult<Oid> {
    lsyscache_seams::get_negator::call(opno)
}

/// Node `equal()` (`equalfuncs.c`) via the central seam (panics until
/// equalfuncs lands — the same boundary `util-vars`/`clauses` cross).
#[inline]
fn equal_node(a: &Expr, b: &Expr) -> bool {
    equalfuncs_seams::equal_expr::call(a, b)
}

/// `is_orclause(clause)` — `IsA(clause, BoolExpr) && boolop == OR_EXPR`.
#[inline]
fn is_orclause(clause: &Expr) -> bool {
    clause.as_boolexpr().is_some_and(|be| be.boolop == OR_EXPR)
}

/// `is_andclause(clause)`.
#[inline]
fn is_andclause(clause: &Expr) -> bool {
    clause.as_boolexpr().is_some_and(|be| be.boolop == AND_EXPR)
}

// ---------------------------------------------------------------------------
// Generic (equal()-based) list set operations. These mirror the PostgreSQL
// `list.c` generic `List *` functions used by `process_duplicate_ors`:
// `list_member`, `list_union`, `list_difference`. The generic forms compare
// elements with node `equal()`, NOT pointer identity, so they are implemented
// here over `&[Expr]` / `Vec<Expr>`, calling the central `equal_node`.
// ---------------------------------------------------------------------------

/// `list_member(list, datum)` — generic list membership using `equal()`.
fn list_member(list: &[Expr], datum: &Expr) -> bool {
    for cell in list {
        if equal_node(cell, datum) {
            return true;
        }
    }
    false
}

/// `list_union(list1, list2)` — generic list union using `equal()`. The C call
/// site is `list_union(NIL, reference)`, so `list1` is always empty. C appends
/// each not-already-present cell's pointer (no node copy); the owned model moves
/// each surviving element from `list2` into the result (consuming `list2`),
/// which avoids deep-cloning Aggref-bearing clauses.
fn list_union<'mcx>(mut result: Vec<Expr<'mcx>>, list2: Vec<Expr<'mcx>>) -> Vec<Expr<'mcx>> {
    for cell in list2 {
        if !list_member(&result, &cell) {
            result.push(cell);
        }
    }
    result
}

/// `list_difference(list1, list2)` — generic list difference using `equal()`:
/// the elements of `list1` not `equal()` to any element of `list2`. Consumes
/// `list1` (its surviving elements move into the result).
fn list_difference<'mcx>(list1: Vec<Expr<'mcx>>, list2: &[Expr<'mcx>]) -> Vec<Expr<'mcx>> {
    /* C: if (list2 == NIL) return list_copy(list1); */
    if list2.is_empty() {
        return list1;
    }

    let mut result: Vec<Expr<'mcx>> = Vec::new();
    for datum in list1 {
        if !list_member(list2, &datum) {
            result.push(datum);
        }
    }
    result
}

// ===========================================================================
//		Public / static functions (prepqual.c)
// ===========================================================================

/// `negate_clause`
///   Negate a Boolean expression.
///
/// Input is a clause to be negated (e.g., the argument of a NOT clause).
/// Returns a new clause equivalent to the negation of the given clause.
///
/// C: `Node *negate_clause(Node *node)`                            (prepqual.c:72)
///
/// The owned tree has no NULL `Expr`; C only reaches this function with a
/// non-NULL node (the `if (node == NULL) elog(ERROR, ...)` guard is enforced
/// by [`negate_clause_opt`] for callers holding an `Option`), so the API
/// takes an owned `Expr`.
pub fn negate_clause<'mcx>(node: Expr<'mcx>) -> PgResult<Expr<'mcx>> {
    match node {
        Expr::Const(c) => {
            /* NOT NULL is still NULL */
            if c.constisnull {
                return Ok(Expr::Const(make_bool_const(false, true)));
            }
            /* otherwise pretty easy */
            Ok(Expr::Const(make_bool_const(
                !datum_get_bool(&c.constvalue),
                false,
            )))
        }
        Expr::OpExpr(opexpr) => {
            /*
             * Negate operator if possible: (NOT (< A B)) => (>= A B)
             */
            let negator = get_negator(opexpr.opno)?;

            if negator != InvalidOid {
                /*
                 * C builds a fresh OpExpr and copies over opresulttype,
                 * opretset, opcollid, inputcollid, args, location verbatim; the
                 * owned-tree analogue reuses the moved node and rewrites the two
                 * fields that change (opno := negator, opfuncid := InvalidOid).
                 */
                let mut newopexpr = opexpr;
                newopexpr.opno = negator;
                newopexpr.opfuncid = InvalidOid;
                Ok(Expr::OpExpr(newopexpr))
            } else {
                /* fall through to the default (tack on a NOT) */
                Ok(make_notclause(Expr::OpExpr(opexpr)))
            }
        }
        Expr::ScalarArrayOpExpr(saopexpr) => {
            /*
             * Negate a ScalarArrayOpExpr if its operator has a negator;
             * for example x = ANY (list) becomes x <> ALL (list)
             */
            let negator = get_negator(saopexpr.opno)?;

            if negator != InvalidOid {
                let mut newopexpr = saopexpr;
                newopexpr.opno = negator;
                newopexpr.opfuncid = InvalidOid;
                newopexpr.hashfuncid = InvalidOid;
                newopexpr.negfuncid = InvalidOid;
                newopexpr.useOr = !newopexpr.useOr;
                /* inputcollid, args, location carried over (moved). */
                Ok(Expr::ScalarArrayOpExpr(newopexpr))
            } else {
                Ok(make_notclause(Expr::ScalarArrayOpExpr(saopexpr)))
            }
        }
        Expr::BoolExpr(expr) => {
            match expr.boolop {
                /*--------------------
                 * Apply DeMorgan's Laws:
                 *		(NOT (AND A B)) => (OR (NOT A) (NOT B))
                 *		(NOT (OR A B))	=> (AND (NOT A) (NOT B))
                 * i.e., swap AND for OR and negate each subclause.
                 *
                 * If the input is already AND/OR flat and has no NOT
                 * directly above AND or OR, this transformation preserves
                 * those properties.  For example, if no direct child of
                 * the given AND clause is an AND or a NOT-above-OR, then
                 * the recursive calls of negate_clause() can't return any
                 * OR clauses.  So we needn't call pull_ors() before
                 * building a new OR clause.  Similarly for the OR case.
                 *--------------------
                 */
                AND_EXPR => {
                    let mut nargs: Vec<Expr> = Vec::new();

                    for lc in expr.args {
                        nargs.push(negate_clause(lc)?);
                    }
                    Ok(make_orclause(nargs))
                }
                OR_EXPR => {
                    let mut nargs: Vec<Expr> = Vec::new();

                    for lc in expr.args {
                        nargs.push(negate_clause(lc)?);
                    }
                    Ok(make_andclause(nargs))
                }
                NOT_EXPR => {
                    /*
                     * NOT underneath NOT: they cancel.  We assume the
                     * input is already simplified, so no need to recurse.
                     */
                    let mut args = expr.args;
                    Ok(args.swap_remove(0)) /* linitial(expr->args) */
                }
            }
        }
        Expr::NullTest(expr) => {
            /*
             * In the rowtype case, the two flavors of NullTest are *not*
             * logical inverses, so we can't simplify.  But it does work
             * for scalar datatypes.
             */
            if !expr.argisrow {
                let newexpr = NullTest {
                    arg: expr.arg,
                    nulltesttype: if expr.nulltesttype == NullTestType::IS_NULL {
                        NullTestType::IS_NOT_NULL
                    } else {
                        NullTestType::IS_NULL
                    },
                    argisrow: expr.argisrow,
                    location: expr.location,
                };
                Ok(Expr::NullTest(newexpr))
            } else {
                /* fall through to the default (tack on a NOT) */
                Ok(make_notclause(Expr::NullTest(expr)))
            }
        }
        Expr::BooleanTest(expr) => {
            let booltesttype = match expr.booltesttype {
                BoolTestType::IS_TRUE => BoolTestType::IS_NOT_TRUE,
                BoolTestType::IS_NOT_TRUE => BoolTestType::IS_TRUE,
                BoolTestType::IS_FALSE => BoolTestType::IS_NOT_FALSE,
                BoolTestType::IS_NOT_FALSE => BoolTestType::IS_FALSE,
                BoolTestType::IS_UNKNOWN => BoolTestType::IS_NOT_UNKNOWN,
                BoolTestType::IS_NOT_UNKNOWN => BoolTestType::IS_UNKNOWN,
            };
            let newexpr = BooleanTest {
                arg: expr.arg,
                booltesttype,
                location: expr.location,
            };
            Ok(Expr::BooleanTest(newexpr))
        }
        /* else fall through */
        other => {
            /*
             * Otherwise we don't know how to simplify this, so just tack on an
             * explicit NOT node.
             */
            Ok(make_notclause(other))
        }
    }
}

/// `negate_clause` with the faithful `node == NULL` guard (C: prepqual.c:74).
///
/// C's `negate_clause(Node *node)` opens with
/// `if (node == NULL) elog(ERROR, "can't negate an empty subexpression");`.
/// The owned `Expr` has no NULL, so callers holding an `Option<Expr>` route
/// through this wrapper, which performs the NULL check and then delegates to
/// [`negate_clause`].
pub fn negate_clause_opt<'mcx>(node: Option<Expr<'mcx>>) -> PgResult<Expr<'mcx>> {
    match node {
        None /* should not happen */ => {
            /* C: elog(ERROR, "can't negate an empty subexpression") */
            Err(PgError::error("can't negate an empty subexpression"))
        }
        Some(node) => negate_clause(node),
    }
}

/// `canonicalize_qual`
///   Convert a qualification expression to the most useful form.
///
/// C: `Expr *canonicalize_qual(Expr *qual, bool is_check)`        (prepqual.c:293)
///
/// `qual` is an `Expr *` that may be NULL → modeled as `Option<Expr>`. Returns
/// the modified qualification (also `Option<Expr>` since an empty input maps to
/// an empty output).
pub fn canonicalize_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: Option<Expr<'mcx>>,
    is_check: bool,
) -> PgResult<Option<Expr<'mcx>>> {
    /* Quick exit for empty qual */
    let qual = match qual {
        None => return Ok(None),
        Some(q) => q,
    };

    /*
     * This should not be invoked on quals in implicit-AND format. In C this is
     * `Assert(!(qual && IsA(qual, List)))`; the owned `Expr` model has no List
     * variant, so the implicit-AND form cannot reach here.
     */

    /*
     * Pull up redundant subclauses in OR-of-AND trees.  We do this only
     * within the top-level AND/OR structure; there's no point in looking
     * deeper.  Also remove any NULL constants in the top-level structure.
     */
    let newqual = find_duplicate_ors(mcx, qual, is_check)?;

    Ok(Some(newqual))
}

/// `pull_ands`
///   Recursively flatten nested AND clauses into a single and-clause list.
///
/// Input is the arglist of an AND clause.
///
/// C: `static List *pull_ands(List *andlist)`                     (prepqual.c:322)
fn pull_ands<'mcx>(andlist: Vec<Expr<'mcx>>) -> Vec<Expr<'mcx>> {
    let mut out_list: Vec<Expr> = Vec::new();

    for subexpr in andlist {
        if is_andclause(&subexpr) {
            /* subexpr is a BoolExpr(AND); recurse on its args. */
            let be = subexpr.expect_into_boolexpr();
            out_list.extend(pull_ands(be.args)); /* list_concat */
        } else {
            out_list.push(subexpr); /* lappend */
        }
    }
    out_list
}

/// `pull_ors`
///   Recursively flatten nested OR clauses into a single or-clause list.
///
/// Input is the arglist of an OR clause.
///
/// C: `static List *pull_ors(List *orlist)`                       (prepqual.c:349)
fn pull_ors<'mcx>(orlist: Vec<Expr<'mcx>>) -> Vec<Expr<'mcx>> {
    let mut out_list: Vec<Expr> = Vec::new();

    for subexpr in orlist {
        if is_orclause(&subexpr) {
            let be = subexpr.expect_into_boolexpr();
            out_list.extend(pull_ors(be.args)); /* list_concat */
        } else {
            out_list.push(subexpr); /* lappend */
        }
    }
    out_list
}

/// `find_duplicate_ors`
///   Given a qualification tree with the NOTs pushed down, search for
///   OR clauses to which the inverse OR distributive law might apply.
///   Only the top-level AND/OR structure is searched.
///
/// While at it, we remove any NULL constants within the top-level AND/OR
/// structure, eg in a WHERE clause, "x OR NULL::boolean" is reduced to "x".
///
/// C: `static Expr *find_duplicate_ors(Expr *qual, bool is_check)` (prepqual.c:406)
fn find_duplicate_ors<'mcx>(mcx: Mcx<'mcx>, qual: Expr<'mcx>, is_check: bool) -> PgResult<Expr<'mcx>> {
    if is_orclause(&qual) {
        let be = qual.expect_into_boolexpr();
        let mut orlist: Vec<Expr> = Vec::new();

        /* Recurse */
        for arg in be.args {
            let arg = find_duplicate_ors(mcx, arg, is_check)?;

            /* Get rid of any constant inputs */
            if let Some(carg) = arg.as_const() {
                if is_check {
                    /* Within OR in CHECK, drop constant FALSE */
                    if !carg.constisnull && !datum_get_bool(&carg.constvalue) {
                        continue;
                    }
                    /* Constant TRUE or NULL, so OR reduces to TRUE */
                    return Ok(Expr::Const(make_bool_const(true, false)));
                } else {
                    /* Within OR in WHERE, drop constant FALSE or NULL */
                    if carg.constisnull || !datum_get_bool(&carg.constvalue) {
                        continue;
                    }
                    /* Constant TRUE, so OR reduces to TRUE */
                    return Ok(arg);
                }
            }

            orlist.push(arg);
        }

        /* Flatten any ORs pulled up to just below here */
        let orlist = pull_ors(orlist);

        /* Now we can look for duplicate ORs */
        process_duplicate_ors(mcx, orlist)
    } else if is_andclause(&qual) {
        let be = qual.expect_into_boolexpr();
        let mut andlist: Vec<Expr> = Vec::new();

        /* Recurse */
        for arg in be.args {
            let arg = find_duplicate_ors(mcx, arg, is_check)?;

            /* Get rid of any constant inputs */
            if let Some(carg) = arg.as_const() {
                if is_check {
                    /* Within AND in CHECK, drop constant TRUE or NULL */
                    if carg.constisnull || datum_get_bool(&carg.constvalue) {
                        continue;
                    }
                    /* Constant FALSE, so AND reduces to FALSE */
                    return Ok(arg);
                } else {
                    /* Within AND in WHERE, drop constant TRUE */
                    if !carg.constisnull && datum_get_bool(&carg.constvalue) {
                        continue;
                    }
                    /* Constant FALSE or NULL, so AND reduces to FALSE */
                    return Ok(Expr::Const(make_bool_const(false, false)));
                }
            }

            andlist.push(arg);
        }

        /* Flatten any ANDs introduced just below here */
        let mut andlist = pull_ands(andlist);

        /* AND of no inputs reduces to TRUE */
        if andlist.is_empty() {
            return Ok(Expr::Const(make_bool_const(true, false)));
        }

        /* Single-expression AND just reduces to that expression */
        if andlist.len() == 1 {
            return Ok(andlist.swap_remove(0)); /* linitial(andlist) */
        }

        /* Else we still need an AND node */
        Ok(make_andclause(andlist))
    } else {
        Ok(qual)
    }
}

/// `process_duplicate_ors`
///   Given a list of exprs which are ORed together, try to apply
///   the inverse OR distributive law.
///
/// Returns the resulting expression (could be an AND clause, an OR
/// clause, or maybe even a single subexpression).
///
/// C: `static Expr *process_duplicate_ors(List *orlist)`         (prepqual.c:517)
fn process_duplicate_ors<'mcx>(mcx: Mcx<'mcx>, mut orlist: Vec<Expr<'mcx>>) -> PgResult<Expr<'mcx>> {
    /* OR of no inputs reduces to FALSE */
    if orlist.is_empty() {
        return Ok(Expr::Const(make_bool_const(false, false)));
    }

    /* Single-expression OR just reduces to that expression */
    if orlist.len() == 1 {
        return Ok(orlist.swap_remove(0)); /* linitial(orlist) */
    }

    /*
     * Choose the shortest AND clause as the reference list --- obviously, any
     * subclause not in this clause isn't in all the clauses. If we find a
     * clause that's not an AND, we can treat it as a one-element AND clause,
     * which necessarily wins as shortest.
     *
     * `reference` in C aliases either an existing AND clause's `args` (no copy)
     * or `list_make1(clause)`. Here we materialize the chosen list as an owned
     * `Vec<Expr>` of clones (a read-only reference set), which matches C's
     * usage: `reference` is only read, then immediately deduplicated by
     * `list_union(NIL, reference)` (which itself copies).
     */
    let mut reference: Option<Vec<Expr>> = None;
    let mut num_subclauses: i32 = 0;
    for clause in &orlist {
        if is_andclause(clause) {
            let be = clause.as_boolexpr().expect("is_andclause guaranteed BoolExpr");
            let subclauses = &be.args;
            let nclauses = subclauses.len() as i32;

            if reference.is_none() || nclauses < num_subclauses {
                // C aliases the AND clause's `args` list (no copy). The owned
                // `Expr` model must materialize a list; deep-copy each subclause
                // into `mcx` (a plain `.clone()` panics on Aggref-bearing
                // children — e.g. `min(a) = max(a)` in a HAVING qual).
                let mut copied: Vec<Expr> = Vec::with_capacity(subclauses.len());
                for sub in subclauses {
                    copied.push(sub.clone_in(mcx)?);
                }
                reference = Some(copied);
                num_subclauses = nclauses;
            }
        } else {
            /* list_make1(clause): C stores the same pointer; the owned model
             * deep-copies into mcx (clone_in handles Aggref children). */
            reference = Some(vec![clause.clone_in(mcx)?]);
            break;
        }
    }
    let reference = reference.unwrap_or_default();

    /*
     * Just in case, eliminate any duplicates in the reference list.
     */
    let reference = list_union(Vec::new(), reference);

    /*
     * Check each element of the reference list to see if it's in all the OR
     * clauses.  Build a new list of winning clauses.
     */
    let mut winners: Vec<Expr> = Vec::new();
    for refclause in &reference {
        let mut win = true;

        for clause in &orlist {
            if is_andclause(clause) {
                let be = clause.as_boolexpr().expect("is_andclause guaranteed BoolExpr");
                if !list_member(&be.args, refclause) {
                    win = false;
                    break;
                }
            } else {
                if !equal_node(refclause, clause) {
                    win = false;
                    break;
                }
            }
        }

        if win {
            // Deep-copy into mcx (clone_in handles Aggref-bearing clauses; a
            // plain `.clone()` panics on them).
            winners.push(refclause.clone_in(mcx)?);
        }
    }

    /*
     * If no winners, we can't transform the OR
     */
    if winners.is_empty() {
        return Ok(make_orclause(orlist));
    }

    /*
     * Generate new OR list consisting of the remaining sub-clauses.
     *
     * If any clause degenerates to empty, then we have a situation like (A
     * AND B) OR (A), which can be reduced to just A --- that is, the
     * additional conditions in other arms of the OR are irrelevant.
     *
     * Note that because we use list_difference, any multiple occurrences of a
     * winning clause in an AND sub-clause will be removed automatically.
     */
    let mut neworlist: Vec<Expr> = Vec::new();
    /* `orlist` is consumed here (its clauses move into `neworlist` / are
     * dropped), matching the end-of-life of the C `orlist` after this loop. */
    for clause in core::mem::take(&mut orlist) {
        if is_andclause(&clause) {
            let be = clause.expect_into_boolexpr();
            let subclauses = list_difference(be.args, &winners);
            if !subclauses.is_empty() {
                let mut subclauses = subclauses;
                if subclauses.len() == 1 {
                    neworlist.push(subclauses.swap_remove(0)); /* linitial */
                } else {
                    neworlist.push(make_andclause(subclauses));
                }
            } else {
                neworlist = Vec::new(); /* degenerate case, see above */
                break;
            }
        } else {
            if !list_member(&winners, &clause) {
                neworlist.push(clause);
            } else {
                neworlist = Vec::new(); /* degenerate case, see above */
                break;
            }
        }
    }

    /*
     * Append reduced OR to the winners list, if it's not degenerate, handling
     * the special case of one element correctly (can that really happen?).
     * Also be careful to maintain AND/OR flatness in case we pulled up a
     * sub-sub-OR-clause.
     */
    if !neworlist.is_empty() {
        if neworlist.len() == 1 {
            winners.push(neworlist.swap_remove(0)); /* linitial(neworlist) */
        } else {
            winners.push(make_orclause(pull_ors(neworlist)));
        }
    }

    /*
     * And return the constructed AND clause, again being wary of a single
     * element and AND/OR flatness.
     */
    if winners.len() == 1 {
        Ok(winners.swap_remove(0)) /* linitial(winners) */
    } else {
        Ok(make_andclause(pull_ands(winners)))
    }
}

/// Install the inward seams this unit owns (`prepqual.c`).
pub fn init_seams() {
    prepqual_seams::negate_clause::set(negate_clause);
    prepqual_seams::canonicalize_qual::set(canonicalize_qual);
}

#[cfg(test)]
mod tests;
