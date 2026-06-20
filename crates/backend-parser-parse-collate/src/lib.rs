//! Port of `src/backend/parser/parse_collate.c` (PostgreSQL 18.3) — assign
//! collation information in completed parse trees.
//!
//! Collation analysis is a post-pass over the output of expression parse
//! analysis: each expression node is annotated with (1) its output collation
//! ([`expr_set_collation`]) and (2) the collation a collation-using function
//! called by the node should use ([`expr_set_input_collation`]). This crate is a
//! 1:1 port of the recursive walker that computes those.
//!
//! # Public functions (C name → Rust)
//!
//! * [`assign_query_collations`] — walk a `Query`, assigning collations.
//! * [`assign_list_collations`] — per-expression assignment over a list.
//! * [`assign_expr_collations`] — assign collations in one expression tree.
//! * [`select_common_collation`] — pick the common collation of a list of
//!   expressions (the SQL collation-combination rules).
//!
//! The static helpers (`assign_query_collations_walker`,
//! `assign_collations_walker`, `merge_collation_state`,
//! `assign_aggregate_collations`, `assign_ordered_set_collations`,
//! `assign_hypothetical_collations`) are ported as crate-private functions;
//! [`AssignCollationsContext`] and the `COLLATE_*` strength enum are modeled
//! below exactly as in C.
//!
//! # The split Expr/Node model
//!
//! This repo carries every `Expr`-derived expression node inside the single
//! `Node::Expr(Expr)` arm; the parse/raw/`Query`/`TargetEntry` vocabulary
//! occupies its own `Node` arms. The C walker threads its state through `void
//! *context` and mutates the tree in place (`exprSetCollation`) while recursing:
//!
//! * a node is a `&mut Node` (mutation in place) and the context is a `&mut
//!   AssignCollationsContext` captured by the recursion;
//! * `expression_tree_walker(node, assign_collations_walker, &loccontext)`
//!   becomes [`backend_nodes_core::node_walker::expression_tree_walker_mut`]
//!   driven by a closure that recurses with `assign_collations_walker(child,
//!   loccontext)` — each child is mutated and merged into `loccontext` exactly
//!   as C does;
//! * the node-level `exprType`/`exprCollation`/`exprSetCollation`/
//!   `exprSetInputCollation`/`exprLocation` accessors are the `Expr`-level
//!   [`backend_nodes_core::nodefuncs`] functions reached *through* the
//!   `Node::Expr(Expr)` arm (the general/default arms of the C switch only ever
//!   handle expression-bearing nodes);
//! * a C `List *` handed to the walker (the `RowExpr`/`WindowFunc` arg lists, the
//!   direct `T_List` entry of `select_common_collation`) is a slice/`Vec` in the
//!   owned tree, walked by [`assign_collations_list_walker`] (the faithful
//!   equivalent of the `T_List` switch arm).
//!
//! # Trimmed-model notes (faithful divergences, documented inline)
//!
//! * The model trims the per-node `location` field repo-wide (docs/types.md rule
//!   3): every `exprLocation` resolves to `-1`, `CollateExpr` has no `location`
//!   field (so the C `expr->location` read is `-1`), and `parser_errposition`
//!   with `-1` contributes cursor 0. The collation logic is location-independent
//!   except for error-message cursors, which collapse to "no cursor" — exactly
//!   the documented fallback.
//! * The C `T_TargetEntry` "indeterminate collation for a sort/group target"
//!   eager error fires here: `TargetEntry` carries `ressortgroupref`, so the
//!   COLLATE_CONFLICT + `ressortgroupref != 0` throw is reproduced exactly.
//!
//! No `extern "C"`, no raw pointers; soft errors flow through `PgResult`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERROR, ERRCODE_COLLATION_MISMATCH, ERRCODE_OUT_OF_MEMORY};
use types_parsenodes::{AGGKIND_HYPOTHETICAL, AGGKIND_NORMAL, AGGKIND_ORDERED_SET};
use types_cluster::ParseState;
use types_nodes::nodes::{ntag, Node};
use types_nodes::primnodes::{Aggref, CoercionForm, Expr};

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::make_relabel_type;
use backend_nodes_core::node_walker::expression_tree_walker_mut;
use backend_nodes_core::nodefuncs::{
    expr_collation, expr_location, expr_set_collation, expr_set_input_collation, expr_type,
    expr_typmod,
};
use backend_utils_cache_lsyscache_seams as lsyscache;

/// `DEFAULT_COLLATION_OID` (`pg_collation.dat` oid 100) — the database default
/// collation. Spelled here from the catalog header (types-core exports
/// `C_COLLATION_OID` but not this one), matching nodeFuncs.c's local use.
const DEFAULT_COLLATION_OID: Oid = 100;

#[cfg(test)]
mod tests;

// ===========================================================================
// CollateStrength and the AssignCollationsContext (parse_collate.c:56-73).
// ===========================================================================

/// `CollateStrength` (parse_collate.c:56): collation "derivation". Variant order
/// matters: it makes the numeric comparisons C performs in
/// [`merge_collation_state`] work directly (later variants dominate).
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
enum CollateStrength {
    /// COLLATE_NONE — expression is of a noncollatable datatype.
    None = 0,
    /// COLLATE_IMPLICIT — collation was derived implicitly.
    Implicit = 1,
    /// COLLATE_CONFLICT — we had a conflict of implicit collations.
    Conflict = 2,
    /// COLLATE_EXPLICIT — collation was derived explicitly.
    Explicit = 3,
}

use CollateStrength::Conflict as COLLATE_CONFLICT;
use CollateStrength::Explicit as COLLATE_EXPLICIT;
use CollateStrength::Implicit as COLLATE_IMPLICIT;
use CollateStrength::None as COLLATE_NONE;

/// `assign_collations_context` (parse_collate.c:64): the state threaded through
/// the collation-assignment tree walk. The C `ParseState *pstate` (used only for
/// error positions) is modeled as the `Option<&ParseState<'_>>` the walk carries;
/// `None` ↔ the C NULL pstate.
struct AssignCollationsContext<'p, 'mcx> {
    /// parse state (for error reporting); `None` ↔ C NULL pstate. The borrow
    /// (`'p`) and the parse state's arena lifetime (`'mcx`) are independent —
    /// the unified `ParseState<'mcx>` is invariant over its arena, so they must
    /// not be unified.
    pstate: Option<&'p ParseState<'mcx>>,
    /// OID of current collation, if any.
    collation: Oid,
    /// strength of current collation choice.
    strength: CollateStrength,
    /// location of expr that set collation.
    location: i32,
    /// OID of conflicting collation (only valid when strength == CONFLICT).
    collation2: Oid,
    /// location of expr that set collation2.
    location2: i32,
    /// Arena the in-place `Node` walker uses to wrap `Expr` children as opaque
    /// `Node`s. C uses raw pointers and needs no arena; the port's walker does.
    /// When `pstate` is `Some`, the arena is recovered from it (the walked tree
    /// lives in the query mcx); when `pstate` is `None` (utility commands /
    /// SQL-function inlining that process an expression without a `ParseState`),
    /// the caller supplies it here.
    mcx_override: Option<mcx::Mcx<'mcx>>,
}

impl<'p, 'mcx> AssignCollationsContext<'p, 'mcx> {
    /// A fresh context for a new tree walk (parse_collate.c:182-186,213-216).
    fn fresh(pstate: Option<&'p ParseState<'mcx>>) -> Self {
        Self::fresh_in(pstate, None)
    }

    /// A fresh context that carries an explicit walker arena, for the
    /// `pstate == None` callers (the arena cannot be recovered from a pstate).
    fn fresh_in(
        pstate: Option<&'p ParseState<'mcx>>,
        mcx_override: Option<mcx::Mcx<'mcx>>,
    ) -> Self {
        Self {
            pstate,
            collation: InvalidOid,
            strength: COLLATE_NONE,
            location: -1,
            // Set these fields just to suppress uninitialized-value warnings:
            collation2: InvalidOid,
            location2: -1,
            mcx_override,
        }
    }

    /// A fresh per-level child context (parse_collate.c: each recursion level
    /// gets its own local `loccontext`). Inherits the parent's pstate **and**
    /// the walker-arena override so the `pstate == None` arena survives nested
    /// recursion (the C code carries the pstate through `context->pstate`; the
    /// override rides alongside for the same reason).
    fn fresh_child(&self) -> Self {
        Self::fresh_in(self.pstate, self.mcx_override)
    }

    /// The arena the in-place `Node` walker wraps `Expr` children in: the
    /// query mcx (recovered from the pstate), or the caller-supplied override
    /// when there is no pstate.
    fn walker_mcx(&self) -> mcx::Mcx<'mcx> {
        if let Some(pstate) = self.pstate {
            *pstate.p_rtable.allocator()
        } else {
            self.mcx_override.expect(
                "assign_collations: a None pstate requires an explicit walker arena \
                 (use assign_expr_collations_in)",
            )
        }
    }
}

// ===========================================================================
// Public entry points (parse_collate.c:100-241).
// ===========================================================================

/// `assign_query_collations()` (parse_collate.c:100): mark all expressions in
/// the given `Query` with collation information.
///
/// Applied to each `Query` after expression parse analysis. We do not recurse
/// into sub-Queries (those were processed when built), and we skip the range
/// table and CTE subqueries (`QTW_IGNORE_RANGE_TABLE | QTW_IGNORE_CTE_SUBQUERIES`).
///
/// C drives this with `query_tree_walker(query, assign_query_collations_walker,
/// pstate, flags)`. Because that post-pass **mutates** the tree
/// (`assign_query_collations_walker` → `exprSetCollation`), the equivalent
/// traversal is reproduced here in mutating form over the same `Query`
/// expression-bearing fields, in the same order and honoring the same `QTW_*`
/// flags. Each `WALK(list)` of `TargetEntry`/expression nodes dispatches to
/// `assign_list_collations`; each `WALK(Node *)` to `assign_expr_collations`
/// (the two branches of `assign_query_collations_walker`).
pub fn assign_query_collations<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    query: &mut types_nodes::copy_query::Query<'mcx>,
) -> PgResult<()> {
    // targetList / returningList are `Vec<TargetEntry>` (typed); each member's
    // `expr` is the independent subexpression the C `T_List` branch of
    // assign_query_collations_walker hands to assign_list_collations.
    for te in query.targetList.iter_mut() {
        if let Some(expr) = te.expr.as_deref_mut() {
            assign_query_collations_walker_expr(pstate, expr)?;
        }
    }
    for wco in query.withCheckOptions.iter_mut() {
        assign_query_collations_walker_node(pstate, wco)?;
    }
    if let Some(oc) = query.onConflict.as_deref_mut() {
        assign_onconflict_collations(pstate, oc)?;
    }
    for ma in query.mergeActionList.iter_mut() {
        assign_query_collations_walker_node(pstate, ma)?;
    }
    if let Some(mjc) = query.mergeJoinCondition.as_deref_mut() {
        assign_query_collations_walker_expr(pstate, mjc)?;
    }
    for te in query.returningList.iter_mut() {
        if let Some(expr) = te.expr.as_deref_mut() {
            assign_query_collations_walker_expr(pstate, expr)?;
        }
    }
    if let Some(jt) = query.jointree.as_deref_mut() {
        assign_fromexpr_collations(pstate, jt)?;
    }
    if let Some(so) = query.setOperations.as_deref_mut() {
        assign_query_collations_walker_node(pstate, so)?;
    }
    if let Some(hq) = query.havingQual.as_deref_mut() {
        assign_query_collations_walker_expr(pstate, hq)?;
    }
    if let Some(lo) = query.limitOffset.as_deref_mut() {
        assign_query_collations_walker_expr(pstate, lo)?;
    }
    if let Some(lc) = query.limitCount.as_deref_mut() {
        assign_query_collations_walker_expr(pstate, lc)?;
    }

    // QTW_EXAMINE_SORTGROUP is NOT set, so the SortGroupClause lists are not
    // visited; but the expressions under WindowClause nodes (start/end offsets)
    // still get walked, matching query_tree_walker's non-sortgroup branch.
    for wc_node in query.windowClause.iter_mut() {
        if let Some(wc) = (**wc_node).as_windowclause_mut() {
            if let Some(start) = wc.startOffset.as_deref_mut() {
                assign_query_collations_walker_node(pstate, start)?;
            }
            if let Some(end) = wc.endOffset.as_deref_mut() {
                assign_query_collations_walker_node(pstate, end)?;
            }
        }
    }

    // QTW_IGNORE_CTE_SUBQUERIES => skip cteList.
    // QTW_IGNORE_RANGE_TABLE => skip the rangetable.
    Ok(())
}

/// `OnConflictExpr` sub-walk of the `assign_query_collations` post-pass: its
/// component expression trees (arbiter where, the SET targetlist, the
/// `WHERE`), each processed independently by `assign_query_collations_walker`.
fn assign_onconflict_collations<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    oce: &mut types_nodes::rawnodes::OnConflictExpr<'mcx>,
) -> PgResult<()> {
    for e in oce.arbiterElems.iter_mut() {
        assign_query_collations_walker_node(pstate, e)?;
    }
    if let Some(w) = oce.arbiterWhere.as_deref_mut() {
        assign_query_collations_walker_node(pstate, w)?;
    }
    for e in oce.onConflictSet.iter_mut() {
        assign_query_collations_walker_node(pstate, e)?;
    }
    if let Some(w) = oce.onConflictWhere.as_deref_mut() {
        assign_query_collations_walker_node(pstate, w)?;
    }
    for e in oce.exclRelTlist.iter_mut() {
        assign_query_collations_walker_node(pstate, e)?;
    }
    Ok(())
}

/// `FromExpr` sub-walk (the query's jointree): each fromlist member and the
/// quals, processed independently.
fn assign_fromexpr_collations<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    from: &mut types_nodes::rawnodes::FromExpr<'mcx>,
) -> PgResult<()> {
    for e in from.fromlist.iter_mut() {
        assign_query_collations_walker_node(pstate, e)?;
    }
    if let Some(q) = from.quals.as_deref_mut() {
        assign_query_collations_walker_node(pstate, q)?;
    }
    Ok(())
}

/// The `WALK(Node *)` branch of `assign_query_collations_walker`
/// (parse_collate.c:125): each expression found is processed independently. C
/// special-cases `SetOperationStmt` (already processed) and a whole `List`
/// (→ assign_list_collations); a bare expression goes to
/// `assign_expr_collations`.
fn assign_query_collations_walker_node<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    node: &mut Node<'mcx>,
) -> PgResult<()> {
    // We don't want to recurse into a set-operations tree; it's already been
    // fully processed in transformSetOperationStmt.
    if node.is_setoperationstmt() {
        return Ok(());
    }
    assign_expr_collations_node(pstate, node)
}

/// The `WALK(Node *)` branch handed an embedded `Expr` (the typed `expr` of a
/// targetlist `TargetEntry`): process it independently via
/// `assign_expr_collations`.
fn assign_query_collations_walker_expr<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    expr: &mut Expr,
) -> PgResult<()> {
    let mut context = AssignCollationsContext::fresh(pstate);
    assign_collations_walker_expr(expr, &mut context)
}

/// `assign_list_collations()` (parse_collate.c:154): mark all nodes in a list of
/// expressions with collation info, processing each independently (they do not
/// have to share a common collation).
pub fn assign_list_collations(pstate: Option<&ParseState<'_>>, exprs: &mut [Expr]) -> PgResult<()> {
    for node in exprs.iter_mut() {
        let mut context = AssignCollationsContext::fresh(pstate);
        assign_collations_walker_expr(node, &mut context)?;
    }
    Ok(())
}

/// `assign_expr_collations()` (parse_collate.c:176): mark all nodes in the given
/// expression tree with collation information.
///
/// Exported for utility commands that process expressions without building a
/// complete `Query`. Should be applied after `transformExpr()` plus any
/// expression-modifying operations such as `coerce_to_boolean()`.
pub fn assign_expr_collations(pstate: Option<&ParseState<'_>>, expr: &mut Expr) -> PgResult<()> {
    // initialize context for tree walk
    let mut context = AssignCollationsContext::fresh(pstate);
    // and away we go
    assign_collations_walker_expr(expr, &mut context)
}

/// `assign_expr_collations()` re-entered from inside an ongoing walk: process
/// `expr` independently (its own fresh per-level context, exactly as the public
/// entry does) but inherit the parent context's walker arena so a `None`-pstate
/// walk keeps its arena across the re-entry.
fn assign_expr_collations_ctx(
    parent: &AssignCollationsContext<'_, '_>,
    expr: &mut Expr,
) -> PgResult<()> {
    let mut context = parent.fresh_child();
    assign_collations_walker_expr(expr, &mut context)
}

/// `assign_list_collations()` re-entered from inside an ongoing walk (same
/// arena-inheritance rationale as [`assign_expr_collations_ctx`]).
fn assign_list_collations_ctx(
    parent: &AssignCollationsContext<'_, '_>,
    exprs: &mut [Expr],
) -> PgResult<()> {
    for node in exprs.iter_mut() {
        let mut context = parent.fresh_child();
        assign_collations_walker_expr(node, &mut context)?;
    }
    Ok(())
}

/// `assign_expr_collations()` for callers that have no [`ParseState`] (a `NULL`
/// pstate in C — utility commands / SQL-function inlining that process a bare
/// expression). C needs no arena because it walks raw pointers; the port's
/// in-place `Node` walker wraps `Expr` children as opaque `Node`s and so needs
/// an arena, which the caller supplies here. Behaviourally identical to
/// `assign_expr_collations(None, expr)` apart from threading the walker arena.
pub fn assign_expr_collations_in<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    expr: &mut Expr,
) -> PgResult<()> {
    let mut context = AssignCollationsContext::fresh_in(None, Some(mcx));
    assign_collations_walker_expr(expr, &mut context)
}

/// `assign_expr_collations()` applied to a generic `Node` (the `void *` the C
/// `assign_query_collations_walker` passes). For the `Node::Expr` arm this is
/// the expression walk; the non-expression `Node` arms (jointree members,
/// MergeAction, …) recurse via the in-place `Node` walker exactly as C's
/// `assign_collations_walker` does for those tags.
fn assign_expr_collations_node<'mcx>(
    pstate: Option<&ParseState<'mcx>>,
    node: &mut Node<'mcx>,
) -> PgResult<()> {
    let mut context = AssignCollationsContext::fresh(pstate);
    assign_collations_walker(node, &mut context)
}

/// `select_common_collation()` (parse_collate.c:208): identify a common
/// collation for a list of expressions (which should all return the same
/// datatype).
///
/// `none_ok` permits returning [`InvalidOid`] when no common collation can be
/// identified; otherwise an error is thrown for a conflict of implicit
/// collations.
pub fn select_common_collation(
    pstate: Option<&ParseState<'_>>,
    exprs: &mut [Expr],
    none_ok: bool,
) -> PgResult<Oid> {
    // initialize context for tree walk
    let mut context = AssignCollationsContext::fresh(pstate);

    // and away we go: invoked directly on the list (the C `T_List` arm).
    assign_collations_list_walker(exprs, &mut context)?;

    // deal with collation conflict
    if context.strength == COLLATE_CONFLICT {
        if none_ok {
            return Ok(InvalidOid);
        }
        return Err(implicit_conflict_error(
            context.pstate,
            context.collation,
            context.collation2,
            context.location2,
        )?);
    }

    // Note: if strength is still COLLATE_NONE, we'll return InvalidOid, but
    // that's okay because it must mean none of the expressions returned
    // collatable datatypes.
    Ok(context.collation)
}

// ===========================================================================
// assign_collations_walker (parse_collate.c:255) and its inner helpers.
// ===========================================================================

/// `assign_collations_walker()` (parse_collate.c:255) over a generic `Node`. For
/// the non-`Expr` `Node` arms the C special-case switch (`T_RowExpr`,
/// `T_TargetEntry`, `T_RangeTblRef`/`T_JoinExpr`/`T_FromExpr`/…, `T_Query`,
/// `T_List`) applies; an embedded `Node::Expr` delegates to
/// [`assign_collations_walker_expr`] (the expression switch).
fn assign_collations_walker<'mcx>(
    node: &mut Node<'mcx>,
    context: &mut AssignCollationsContext<'_, 'mcx>,
) -> PgResult<()> {
    // Prepare for recursion: each level has its own local context.
    let mut loccontext = context.fresh_child();

    let collation: Oid;
    let strength: CollateStrength;
    let location: i32;

    // An embedded expression node: run the full expression switch on it, which
    // itself merges into `context`, so we are done here. (`Node::Expr` is the
    // routing variant; it has no single `nodeTag`, so it is matched as the
    // outer enum shape before the per-tag switch below.)
    if let Some(e) = node.as_expr_mut() {
        return assign_collations_walker_expr(e, context);
    }

    match node.node_tag() {
        ntag::T_TargetEntry => {
            let ressortgroupref = node.expect_targetentry().ressortgroupref;
            recurse_children(node, &mut loccontext)?;

            // TargetEntry can have only one child, and should bubble that state
            // up to its parent.
            collation = loccontext.collation;
            strength = loccontext.strength;
            location = loccontext.location;

            // Throw eagerly for an indeterminate collation on a sort/group
            // target (`ressortgroupref != 0`): we prefer a syntax-error pointer
            // now over a runtime comparison-function failure (parse_collate.c:471).
            if strength == COLLATE_CONFLICT && ressortgroupref != 0 {
                return Err(implicit_conflict_error(
                    context.pstate,
                    loccontext.collation,
                    loccontext.collation2,
                    loccontext.location2,
                )?);
            }
        }
        ntag::T_RangeTblRef
        | ntag::T_JoinExpr
        | ntag::T_FromExpr
        | ntag::T_OnConflictExpr
        | ntag::T_SortGroupClause
        | ntag::T_MergeAction => {
            recurse_children(node, &mut loccontext)?;
            // When invoked on a query's jointree, recurse through join nodes to
            // process WHERE/ON expressions, then stop. Likewise for sort/group
            // lists. (C also lists T_InferenceElem here; it is an Expr in this
            // model and handled with the same recurse-then-return behavior in the
            // Expr switch.)
            return Ok(());
        }
        ntag::T_Query => {
            let q = node.expect_query();
            // Invoked on the Query belonging to a SubLink. Act as though the
            // Query returns its first output column. Special case: EXISTS may
            // return no columns. We needn't recurse, the Query is processed.
            if q.targetList.is_empty() {
                return Ok(());
            }
            let tent = &q.targetList[0];
            if tent.resjunk {
                return Ok(());
            }
            collation = expr_collation(tent.expr.as_deref())?;
            // collation doesn't change if it's converted to array
            strength = COLLATE_IMPLICIT;
            location = expr_location(tent.expr.as_deref())?;
        }
        // C's general/default arms operate only on expression-bearing nodes,
        // which in this model are `Node::Expr` (handled above). Any other `Node`
        // arm reaching the walker is the C default `elog(ERROR, "unrecognized
        // node type")`.
        _ => return Err(unrecognized_node_type(node)),
    }

    // Now, merge my information into my parent's state.
    merge_collation_state(
        collation,
        strength,
        location,
        loccontext.collation2,
        loccontext.location2,
        context,
    )
}

/// `assign_collations_walker()` (parse_collate.c:255), expression switch — the
/// recursive guts over an embedded `Expr`. Bubbles its computed state into
/// `context` via [`merge_collation_state`].
fn assign_collations_walker_expr(
    expr: &mut Expr,
    context: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    let mut loccontext = context.fresh_child();

    let collation: Oid;
    let strength: CollateStrength;
    let location: i32;

    match expr {
        Expr::CollateExpr(_) => {
            // COLLATE sets an explicitly derived collation, regardless of the
            // child state. But we must recurse to set up collation info below.
            recurse_expr_children(expr, &mut loccontext)?;

            let ce = match expr {
                Expr::CollateExpr(ce) => ce,
                _ => unreachable!(),
            };
            collation = ce.collOid;
            debug_assert!(OidIsValid(collation));
            strength = COLLATE_EXPLICIT;
            // `CollateExpr.location` is trimmed (model-wide rule 3) → -1.
            location = -1;
        }
        Expr::FieldSelect(_) => {
            // For FieldSelect, the result has the field's declared collation,
            // independently of the arguments. The field's collation was already
            // looked up and saved in the node.
            recurse_expr_children(expr, &mut loccontext)?;

            let resultcollid = match expr {
                Expr::FieldSelect(fs) => fs.resultcollid,
                _ => unreachable!(),
            };
            if OidIsValid(resultcollid) {
                // Node's result type is collatable.
                collation = resultcollid;
                strength = COLLATE_IMPLICIT;
                location = expr_location(Some(expr))?;
            } else {
                // Node's result type isn't collatable.
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; // won't be used
            }
        }
        Expr::RowExpr(re) => {
            // RowExpr is special: the subexpressions are independent; we don't
            // complain if some have incompatible explicit collations.
            assign_list_collations_ctx(context, &mut re.args)?;
            // The result is always composite, never collatable: stop here.
            return Ok(()); // done
        }
        Expr::RowCompareExpr(rce) => {
            // For RowCompare, find the common collation of each pair of input
            // columns and build a list. If we can't find a common collation,
            // put InvalidOid in the list (may or may not error at runtime).
            //
            // C runs `select_common_collation(pstate, list_make2(le, re), true)`
            // on each pair, walking `le` and `re` in place (marking their
            // collations) and bubbling up their common collation. The owned
            // `largs`/`rargs` are separate `Vec<Expr>`, so we inline
            // `select_common_collation`'s body for the pair, walking the real
            // `&mut largs[i]`/`&mut rargs[i]` into a fresh pair context, then
            // resolving the conflict with `none_ok = true`.
            let pstate = context.pstate;
            let n = rce.largs.len().min(rce.rargs.len());
            let mut colls: Vec<Oid> = Vec::new();
            colls.try_reserve(n).map_err(|_| alloc_failed())?;
            for i in 0..n {
                let mut paircontext = AssignCollationsContext::fresh(pstate);
                assign_collations_walker_expr(&mut rce.largs[i], &mut paircontext)?;
                assign_collations_walker_expr(&mut rce.rargs[i], &mut paircontext)?;
                let coll = if paircontext.strength == COLLATE_CONFLICT {
                    InvalidOid
                } else {
                    paircontext.collation
                };
                colls.push(coll);
            }
            rce.inputcollids = colls;
            // The result is always boolean, never collatable: stop here.
            return Ok(()); // done
        }
        Expr::CoerceToDomain(_) => {
            // If the domain declaration included a non-default COLLATE spec, use
            // that as the output collation of the coercion. Otherwise allow the
            // input collation to bubble up.
            let resulttype = match expr {
                Expr::CoerceToDomain(c) => c.resulttype,
                _ => unreachable!(),
            };
            let typcollation = lsyscache::get_typcollation::call(resulttype)?;

            // ... but first, recurse
            recurse_expr_children(expr, &mut loccontext)?;

            if OidIsValid(typcollation) {
                // Node's result type is collatable.
                if typcollation == DEFAULT_COLLATION_OID {
                    // Collation state bubbles up from child.
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    // Use domain's collation as an implicit choice.
                    collation = typcollation;
                    strength = COLLATE_IMPLICIT;
                    location = expr_location(Some(expr))?;
                }
            } else {
                // Node's result type isn't collatable.
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; // won't be used
            }

            // Save the state into the expression node. We know it doesn't care
            // about input collation.
            if strength == COLLATE_CONFLICT {
                expr_set_collation(expr, InvalidOid)?;
            } else {
                expr_set_collation(expr, collation)?;
            }
        }
        // C's `case T_Var/T_Const/T_Param/T_CoerceToDomainValue/T_CaseTestExpr/
        // T_SetToDefault/T_CurrentOfExpr`: general case for childless expression
        // nodes, already marked when built.
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Param(_)
        | Expr::CoerceToDomainValue(_)
        | Expr::CaseTestExpr(_)
        | Expr::SetToDefault(_)
        | Expr::CurrentOfExpr(_) => {
            collation = expr_collation(Some(expr))?;
            if OidIsValid(collation) {
                strength = COLLATE_IMPLICIT;
            } else {
                strength = COLLATE_NONE;
            }
            location = expr_location(Some(expr))?;
        }
        Expr::InferenceElem(_) => {
            // C groups T_InferenceElem with T_RangeTblRef/T_JoinExpr/etc
            // (parse_collate.c:482): recurse through children to process any
            // embedded expressions, then `return false` — never bubble collation
            // state up to the parent. (InferenceElem is an Expr in this model, so
            // it is reached here rather than in the Node-level switch.)
            recurse_expr_children(expr, &mut loccontext)?;
            return Ok(());
        }
        _ => {
            // General case for most expression nodes with children. First
            // recurse, then figure out what to assign to this node.
            match expr {
                Expr::Aggref(_) => {
                    // Aggref is messy enough that we give it its own functions,
                    // in fact three of them. The FILTER clause is independent of
                    // the rest of the aggregate, so it is processed separately.
                    let aggkind = match expr {
                        Expr::Aggref(a) => a.aggkind,
                        _ => unreachable!(),
                    };
                    {
                        let aggref = match expr {
                            Expr::Aggref(a) => a,
                            _ => unreachable!(),
                        };
                        match aggkind {
                            AGGKIND_NORMAL => {
                                assign_aggregate_collations(aggref, &mut loccontext)?
                            }
                            AGGKIND_ORDERED_SET => {
                                assign_ordered_set_collations(aggref, &mut loccontext)?
                            }
                            AGGKIND_HYPOTHETICAL => {
                                assign_hypothetical_collations(aggref, &mut loccontext)?
                            }
                            other => return Err(unrecognized_aggkind(other)),
                        }
                    }
                    // assign_expr_collations(pstate, (Node *) aggref->aggfilter)
                    let aggref = match expr {
                        Expr::Aggref(a) => a,
                        _ => unreachable!(),
                    };
                    if let Some(aggfilter) = aggref.aggfilter.as_deref_mut() {
                        assign_expr_collations_ctx(context, aggfilter)?;
                    }
                }
                Expr::WindowFunc(_) => {
                    // WindowFunc requires special processing only for its
                    // aggfilter clause, as for aggregates.
                    let wfunc = match expr {
                        Expr::WindowFunc(w) => w,
                        _ => unreachable!(),
                    };
                    assign_collations_list_walker(&mut wfunc.args, &mut loccontext)?;
                    if let Some(aggfilter) = wfunc.aggfilter.as_deref_mut() {
                        assign_expr_collations_ctx(context, aggfilter)?;
                    }
                }
                Expr::CaseExpr(_) => {
                    // CaseExpr is special: we do not recurse into the test
                    // expression (if any). It was marked during transformCaseExpr
                    // and its collation is not relevant to the CASE result — only
                    // the output expressions are.
                    let cexpr = match expr {
                        Expr::CaseExpr(c) => c,
                        _ => unreachable!(),
                    };
                    for when in cexpr.args.iter_mut() {
                        // The condition expressions mustn't affect the CASE's
                        // result collation; but they yield boolean, so it's safe
                        // to recurse directly — they won't change loccontext.
                        if let Some(cond) = when.expr.as_deref_mut() {
                            assign_collations_walker_expr(cond, &mut loccontext)?;
                        }
                        if let Some(result) = when.result.as_deref_mut() {
                            assign_collations_walker_expr(result, &mut loccontext)?;
                        }
                    }
                    if let Some(defresult) = cexpr.defresult.as_deref_mut() {
                        assign_collations_walker_expr(defresult, &mut loccontext)?;
                    }
                }
                Expr::SubscriptingRef(_) => {
                    // The subscripts are independent expressions not contributing
                    // to the node's collation. Only the container, and the source
                    // expression if any, contribute.
                    let sbsref = match expr {
                        Expr::SubscriptingRef(s) => s,
                        _ => unreachable!(),
                    };
                    for e in sbsref.refupperindexpr.iter_mut().flatten() {
                        assign_expr_collations_ctx(context, e)?;
                    }
                    for e in sbsref.reflowerindexpr.iter_mut().flatten() {
                        assign_expr_collations_ctx(context, e)?;
                    }
                    if let Some(refexpr) = sbsref.refexpr.as_deref_mut() {
                        assign_collations_walker_expr(refexpr, &mut loccontext)?;
                    }
                    if let Some(refassgnexpr) = sbsref.refassgnexpr.as_deref_mut() {
                        assign_collations_walker_expr(refassgnexpr, &mut loccontext)?;
                    }
                }
                _ => {
                    // Normal case: all child expressions contribute equally.
                    recurse_expr_children(expr, &mut loccontext)?;
                }
            }

            // Now figure out what collation to assign to this node.
            let typcollation = lsyscache::get_typcollation::call(expr_type(Some(expr))?)?;
            if OidIsValid(typcollation) {
                // Node's result is collatable; what about its input?
                if loccontext.strength > COLLATE_NONE {
                    // Collation state bubbles up from children.
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    // Collatable output produced without any collatable input.
                    // Use the type's collation (usually DEFAULT_COLLATION_OID,
                    // but might differ for a domain).
                    collation = typcollation;
                    strength = COLLATE_IMPLICIT;
                    location = expr_location(Some(expr))?;
                }
            } else {
                // Node's result type isn't collatable.
                collation = InvalidOid;
                strength = COLLATE_NONE;
                location = -1; // won't be used
            }

            // Save the result collation into the expression node. If the state
            // is COLLATE_CONFLICT, set the collation to InvalidOid (might error
            // at runtime).
            if strength == COLLATE_CONFLICT {
                expr_set_collation(expr, InvalidOid)?;
            } else {
                expr_set_collation(expr, collation)?;
            }

            // Likewise save the input collation, used by any function called by
            // this node.
            if loccontext.strength == COLLATE_CONFLICT {
                expr_set_input_collation(expr, InvalidOid);
            } else {
                expr_set_input_collation(expr, loccontext.collation);
            }
        }
    }

    // Now, merge my information into my parent's state.
    merge_collation_state(
        collation,
        strength,
        location,
        loccontext.collation2,
        loccontext.location2,
        context,
    )
}

/// `(void) expression_tree_walker(node, assign_collations_walker, &loccontext)`
/// over a generic `Node` — recurse into children, mutating each and merging its
/// state into `loccontext`.
fn recurse_children<'mcx>(
    node: &mut Node<'mcx>,
    loccontext: &mut AssignCollationsContext<'_, 'mcx>,
) -> PgResult<()> {
    let mut err: Option<types_error::PgError> = None;
    // The in-place walker wraps each `Expr` child as a transient opaque `Node`
    // tied to the walked tree's context (`'mcx`); recover that context from the
    // pstate (the tree being collated lives in the query mcx), or from the
    // caller-supplied override when there is no pstate.
    let mcx: mcx::Mcx<'mcx> = loccontext.walker_mcx();
    expression_tree_walker_mut(
        node,
        &mut |child: &mut Node<'mcx>| match assign_collations_walker(child, loccontext) {
            Ok(()) => false,
            Err(e) => {
                err = Some(e);
                true
            }
        },
        mcx,
    );
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `(void) expression_tree_walker(node, assign_collations_walker, &loccontext)`
/// over an embedded `Expr` — recurse into the expression's children (wrapped as
/// `Node::Expr` by the in-place `Node` walker), mutating each and merging into
/// `loccontext`.
fn recurse_expr_children<'mcx>(
    expr: &mut Expr,
    loccontext: &mut AssignCollationsContext<'_, 'mcx>,
) -> PgResult<()> {
    // Wrap the Expr into an opaque `Node` (allocated in the query mcx, recovered
    // from the pstate, or from the caller-supplied override when there is no
    // pstate) so the Node-level in-place walker enumerates its children. The
    // wrapper is written back so any in-place mutation is preserved.
    let mcx: mcx::Mcx<'mcx> = loccontext.walker_mcx();
    let placeholder = Expr::Var(types_nodes::primnodes::Var::default());
    let mut wrapped = Node::mk_expr(mcx, core::mem::replace(expr, placeholder))?;
    let res = recurse_children(&mut wrapped, loccontext);
    if let Some(e) = wrapped.into_expr() {
        *expr = e;
    }
    res
}

/// The `T_List` arm of `assign_collations_walker` (parse_collate.c:528-540) plus
/// the C "invoked directly on a List" entry (parse_collate.c:219): walk each
/// element, bubbling collation state up from the list elements into `context`.
fn assign_collations_list_walker(
    exprs: &mut [Expr],
    context: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    for expr in exprs.iter_mut() {
        assign_collations_walker_expr(expr, context)?;
    }
    Ok(())
}

/// `merge_collation_state()` (parse_collate.c:780): merge the collation state of
/// a subexpression into the context for its parent.
fn merge_collation_state(
    collation: Oid,
    strength: CollateStrength,
    location: i32,
    collation2: Oid,
    location2: i32,
    context: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    // If the collation strength for this node is different from what's already
    // in *context, then this node either dominates or is dominated by earlier
    // siblings.
    if strength > context.strength {
        // Override previous parent state
        context.collation = collation;
        context.strength = strength;
        context.location = location;
        // Bubble up error info if applicable
        if strength == COLLATE_CONFLICT {
            context.collation2 = collation2;
            context.location2 = location2;
        }
    } else if strength == context.strength {
        // Merge, or detect error if there's a collation conflict
        match strength {
            COLLATE_NONE => {
                // Nothing + nothing is still nothing
            }
            COLLATE_IMPLICIT => {
                if collation != context.collation {
                    // Non-default implicit collation always beats default.
                    if context.collation == DEFAULT_COLLATION_OID {
                        // Override previous parent state
                        context.collation = collation;
                        context.strength = strength;
                        context.location = location;
                    } else if collation != DEFAULT_COLLATION_OID {
                        // Oops, we have a conflict. We cannot throw error here,
                        // since the conflict could be resolved by a later sibling
                        // CollateExpr, or the parent might not care about
                        // collation anyway. Return enough info to throw later.
                        context.strength = COLLATE_CONFLICT;
                        context.collation2 = collation;
                        context.location2 = location;
                    }
                }
            }
            COLLATE_CONFLICT => {
                // We're still conflicted ...
            }
            COLLATE_EXPLICIT => {
                if collation != context.collation {
                    // Oops, we have a conflict of explicit COLLATE clauses. We
                    // throw error immediately; that is what the SQL standard
                    // says to do.
                    return Err(explicit_conflict_error(
                        context.pstate,
                        context.collation,
                        collation,
                        location,
                    )?);
                }
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Aggregate helpers (parse_collate.c:881-1058).
// ===========================================================================

/// `assign_aggregate_collations()` (parse_collate.c:881): collation handling for
/// plain aggregates.
///
/// Expressions used only for ordering shouldn't be taken to conflict with each
/// other or with regular args; we handle this by applying
/// [`assign_expr_collations`] to the resjunk ones rather than passing down our
/// `loccontext`.
fn assign_aggregate_collations(
    aggref: &mut Aggref,
    loccontext: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    // Plain aggregates have no direct args
    debug_assert!(aggref.aggdirectargs.is_empty());

    // Process aggregated args, holding resjunk ones at arm's length. C recurses
    // to each TargetEntry (so the T_TargetEntry arm runs); in this model
    // `aggref.args` is `Vec<TargetEntry>` whose single child is `tle.expr`. The
    // T_TargetEntry arm's only observable effects are (a) bubbling the child's
    // state and (b) the sort/group eager throw — which is dead here (trimmed
    // ressortgroupref). So walking `tle.expr` directly is equivalent to walking
    // the TargetEntry: deref-and-walk the inner Expr (the prompt's adaptation;
    // TargetEntry is not cloned — its Aggref-bearing exprs would panic on clone).
    for tle in aggref.args.iter_mut() {
        let resjunk = tle.resjunk;
        if let Some(e) = tle.expr.as_deref_mut() {
            if resjunk {
                assign_expr_collations_ctx(loccontext, e)?;
            } else {
                assign_collations_walker_expr(e, loccontext)?;
            }
        }
    }
    Ok(())
}

/// `assign_ordered_set_collations()` (parse_collate.c:919): collation handling
/// for ordered-set aggregates.
///
/// Direct arguments contribute normally to the aggregate's own collation, while
/// aggregated arguments contribute only when the aggregate is designed to have
/// exactly one aggregated argument (single aggregated arg and non-variadic).
fn assign_ordered_set_collations(
    aggref: &mut Aggref,
    loccontext: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    // Merge sort collations to parent only if there can be only one
    let merge_sort_collations = aggref.args.len() == 1
        && lsyscache::get_func_variadictype::call(aggref.aggfnoid)? == InvalidOid;

    // Direct args, if any, are normal children of the Aggref node.
    assign_collations_list_walker(&mut aggref.aggdirectargs, loccontext)?;

    // Process aggregated args appropriately (walking `tle.expr`, see
    // assign_aggregate_collations for why the inner Expr is walked directly).
    for tle in aggref.args.iter_mut() {
        if let Some(e) = tle.expr.as_deref_mut() {
            if merge_sort_collations {
                assign_collations_walker_expr(e, loccontext)?;
            } else {
                assign_expr_collations_ctx(loccontext, e)?;
            }
        }
    }
    Ok(())
}

/// `assign_hypothetical_collations()` (parse_collate.c:955): collation handling
/// for hypothetical-set aggregates.
///
/// Per spec, we unify the collations of each pair of hypothetical and aggregated
/// args, and force the choice of collation down into the sort column (via a
/// `RelabelType`, matching `makeRelabelType`) so the sort happens with the
/// chosen collation.
fn assign_hypothetical_collations(
    aggref: &mut Aggref,
    loccontext: &mut AssignCollationsContext<'_, '_>,
) -> PgResult<()> {
    // Merge sort collations to parent only if there can be only one
    let merge_sort_collations = aggref.args.len() == 1
        && lsyscache::get_func_variadictype::call(aggref.aggfnoid)? == InvalidOid;

    // Process any non-hypothetical direct args. C walks `aggdirectargs` and
    // `args` cell-by-cell in parallel; the owned `aggdirectargs: Vec<Expr>` and
    // `args: Vec<TargetEntry>` are indexed in parallel here (h_cell/s_cell).
    let extra_args = aggref.aggdirectargs.len() as isize - aggref.args.len() as isize;
    debug_assert!(extra_args >= 0);

    let extra = (extra_args.max(0) as usize).min(aggref.aggdirectargs.len());
    for h in aggref.aggdirectargs[..extra].iter_mut() {
        assign_collations_walker_expr(h, loccontext)?;
    }

    // Scan hypothetical args and aggregated args in parallel. We mutate both
    // `aggref.aggdirectargs[h_idx]` and `aggref.args[s_idx]`, so split the
    // borrows by taking the args list out, processing, and putting it back.
    let mut s_args = core::mem::take(&mut aggref.args);
    let pair_count = aggref
        .aggdirectargs
        .len()
        .saturating_sub(extra)
        .min(s_args.len());
    let pstate = loccontext.pstate;

    let mut result: PgResult<()> = Ok(());
    'pairs: for k in 0..pair_count {
        let h_idx = extra + k;
        let s_idx = k;

        let h_arg = &mut aggref.aggdirectargs[h_idx];
        let s_tle = &mut s_args[s_idx];

        // Assign collations internally in this pair, then choose a common
        // collation for them. This should match select_common_collation(), but
        // we can't use it as-is because we need the whole collation state to
        // bubble up to the aggregate function's level.
        let mut paircontext = AssignCollationsContext::fresh(pstate);

        if let Err(e) = assign_collations_walker_expr(h_arg, &mut paircontext) {
            result = Err(e);
            break 'pairs;
        }
        if let Some(s_expr) = s_tle.expr.as_deref_mut() {
            if let Err(e) = assign_collations_walker_expr(s_expr, &mut paircontext) {
                result = Err(e);
                break 'pairs;
            }
        }

        // deal with collation conflict
        if paircontext.strength == COLLATE_CONFLICT {
            result = Err(match implicit_conflict_error(
                paircontext.pstate,
                paircontext.collation,
                paircontext.collation2,
                paircontext.location2,
            ) {
                Ok(e) => e,
                Err(e) => e,
            });
            break 'pairs;
        }

        // At this point paircontext.collation can be InvalidOid only if the type
        // is not collatable; no need to do anything then. If we must change the
        // sort column's collation, do it by inserting a RelabelType node into the
        // sort column TLE. (XXX grotty; see the C comment. This relies on
        // ruleutils.c not printing a COLLATE clause for a RelabelType.)
        let s_expr_collation = expr_collation(s_tle.expr.as_deref())?;
        if OidIsValid(paircontext.collation) && paircontext.collation != s_expr_collation {
            if let Some(slot) = s_tle.expr.as_deref_mut() {
                // makeRelabelType(s_tle->expr, exprType, exprTypmod,
                // paircontext.collation, COERCE_IMPLICIT_CAST). Take the old expr
                // out of the box (leaving a placeholder), build the RelabelType
                // around it, and write it back into the same box (no realloc).
                let placeholder = Expr::Var(types_nodes::primnodes::Var::default());
                let old = core::mem::replace(slot, placeholder);
                let rtype = match expr_type(Some(&old)) {
                    Ok(t) => t,
                    Err(e) => {
                        result = Err(e);
                        break 'pairs;
                    }
                };
                let rtypmod = match expr_typmod(Some(&old)) {
                    Ok(t) => t,
                    Err(e) => {
                        result = Err(e);
                        break 'pairs;
                    }
                };
                *slot = make_relabel_type(
                    old,
                    rtype,
                    rtypmod,
                    paircontext.collation,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                );
            }
        }

        // If appropriate, merge this column's collation state up to the
        // aggregate function.
        if merge_sort_collations {
            if let Err(e) = merge_collation_state(
                paircontext.collation,
                paircontext.strength,
                paircontext.location,
                paircontext.collation2,
                paircontext.location2,
                loccontext,
            ) {
                result = Err(e);
                break 'pairs;
            }
        }
    }

    // Put the (now possibly relabeled) aggregated-args list back.
    aggref.args = s_args;
    result?;

    // C asserts both cursors reached NULL together; the index walk enforces it.
    debug_assert_eq!(aggref.aggdirectargs.len(), extra + pair_count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Error builders (kept here so the bodies read like C).
// ---------------------------------------------------------------------------

/// `parser_errposition(pstate, location)` — cursor position for an error from a
/// token location. NULL pstate (None) contributes 0 (cf parse_type.c).
fn parser_errposition(pstate: Option<&ParseState<'_>>, location: i32) -> PgResult<i32> {
    match pstate {
        Some(ps) => backend_parser_small1_seams::parser_errposition::call(ps, location),
        None => Ok(0),
    }
}

/// `get_collation_name(colloid)` (lsyscache.c) — the collation's name as an
/// owned `String` for the error message. The ported `get_collation_name`
/// allocates the name in a context; we use a scratch one and copy out, the same
/// idiom as `format_type_be_owned`. A missing collation row (`None`) prints as
/// C's `%s` of a NULL pointer would — here `(null)`.
fn get_collation_name(colloid: Oid) -> PgResult<String> {
    let scratch = mcx::MemoryContext::new("get_collation_name");
    let mcx = scratch.mcx();
    let name =
        backend_utils_cache_lsyscache::collation_constraint_language_cast::get_collation_name(
            mcx, colloid,
        )?;
    let out = match &name {
        Some(name) => String::from(name.as_str()),
        None => String::from("(null)"),
    };
    Ok(out)
}

/// The implicit-collation conflict ereport shared by `select_common_collation`,
/// the sort/group `TargetEntry` arm, and the hypothetical-pair conflict
/// (parse_collate.c:226-232, 473-480, 1003-1010).
fn implicit_conflict_error(
    pstate: Option<&ParseState<'_>>,
    collation: Oid,
    collation2: Oid,
    location2: i32,
) -> PgResult<types_error::PgError> {
    let name1 = get_collation_name(collation)?;
    let name2 = get_collation_name(collation2)?;
    let cursor = parser_errposition(pstate, location2)?;
    Ok(ereport(ERROR)
        .errcode(ERRCODE_COLLATION_MISMATCH)
        .errmsg(format!(
            "collation mismatch between implicit collations \"{name1}\" and \"{name2}\""
        ))
        .errhint(
            "You can choose the collation by applying the COLLATE clause to one or both expressions.",
        )
        .errposition(cursor)
        .into_error())
}

/// The explicit-collation conflict ereport thrown by `merge_collation_state`
/// (parse_collate.c:853-858). No HINT (matching PG).
fn explicit_conflict_error(
    pstate: Option<&ParseState<'_>>,
    collation: Oid,
    collation2: Oid,
    location: i32,
) -> PgResult<types_error::PgError> {
    let name1 = get_collation_name(collation)?;
    let name2 = get_collation_name(collation2)?;
    let cursor = parser_errposition(pstate, location)?;
    Ok(ereport(ERROR)
        .errcode(ERRCODE_COLLATION_MISMATCH)
        .errmsg(format!(
            "collation mismatch between explicit collations \"{name1}\" and \"{name2}\""
        ))
        .errposition(cursor)
        .into_error())
}

/// `elog(ERROR, "unrecognized aggkind: %d")` (parse_collate.c:615).
fn unrecognized_aggkind(aggkind: i8) -> types_error::PgError {
    ereport(ERROR)
        .errmsg_internal(format!("unrecognized aggkind: {}", aggkind as i32))
        .into_error()
}

/// C default arm of `assign_collations_walker`: `elog(ERROR, "unrecognized node
/// type")`. Reached only for a non-expression `Node` arm the C switch has no
/// case for.
fn unrecognized_node_type(node: &Node) -> types_error::PgError {
    ereport(ERROR)
        .errmsg_internal(format!(
            "assign_collations_walker: unrecognized node type {}",
            node.node_tag()
        ))
        .into_error()
}

fn alloc_failed() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg_internal("out of memory building RowCompareExpr inputcollids")
        .into_error()
}

// ===========================================================================
// init_seams — this unit owns NO inward seam.
// ===========================================================================

/// `init_seams()` — `parse_collate`'s public functions are its API; no other
/// ported crate calls them through a seam (the consumers parse_expr/analyze are
/// unported), so this unit installs nothing. Kept (and NOT wired into
/// `init_all`) for the guard's "every unit has an init_seams" convention, the
/// same as `functioncmds`/`dest`.
pub fn init_seams() {}
