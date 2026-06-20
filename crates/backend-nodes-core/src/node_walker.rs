//! Family: **node-walker engine** — the `Node`-level generic tree recursions
//! from `nodes/nodeFuncs.c` (`expression_tree_walker`, `query_tree_walker`,
//! `range_table_walker`, `range_table_entry_walker`,
//! `raw_expression_tree_walker`, and the `*_mut` in-place variant) plus the
//! statement-level drivers.
//!
//! # Why a separate `Node`-level walker
//!
//! The existing [`crate::nodefuncs::expression_tree_walker`] recurses over the
//! trimmed [`types_nodes::primnodes::Expr`] enum (callback `FnMut(&Expr)`). The
//! parser recursive cluster (`parse_collate`/`expr`/`clause`/`target`/`func`/
//! `agg`/`relation`) walks the *full* `Node` universe — `Query`,
//! `RangeTblEntry`, the raw-grammar statement nodes, and the value/expression
//! leaves — so it needs the C `Node *`-level `expression_tree_walker`
//! (nodeFuncs.c ~2088, ~571 lines) whose callback is `FnMut(&Node)`. These are
//! ADDITIVE: the `Expr`-level walker keeps its signature; this module adds the
//! `Node`-level family alongside.
//!
//! # The split Expr/Node model and child wrapping
//!
//! `types_nodes::Node` keeps the split model — every `Expr`-derived node is
//! carried as the single arm `Node::Expr(Expr)`, while the parse/raw/Query
//! vocabulary occupies its own `Node` arms. A `Node`'s expression children come
//! in two shapes:
//!
//! * a `NodePtr` (`PgBox<Node>`) child — already a `Node`, walked directly
//!   (this is how the parse/raw nodes carry their expression sub-trees);
//! * an owned `Expr` child inside an `Expr` struct (`Box<Expr>`/`Vec<Expr>`/
//!   `PgBox<Expr>`) — wrapped on the fly into a `Node::Expr` so the
//!   `FnMut(&Node)` walker observes it. C visits the in-place `(Node *) child`
//!   *by pointer* and never copies; the split Expr/Node model cannot borrow an
//!   `&Expr` into the owned `Node::Expr(Expr)` arm, so the read-only walker
//!   materializes the wrapper. A bare `Expr::clone()` is wrong: it is a
//!   deliberate panic for the arena-pointer-carrying variants (`Aggref`/
//!   `WindowFunc`/`SubLink`/`SubPlan`/`AlternativeSubPlan` — e.g. the `count(*)`
//!   target list's `Aggref`). Instead [`node_expr_wrapper`] deep-copies the
//!   child into a per-walk scratch `MemoryContext` via the non-panicking
//!   `Expr::clone_in` (the `copyObject`-shape path). The wrapper is only
//!   read-borrowed by the callback (no consumer takes ownership) and freed when
//!   the walk level returns, so a deep copy is observationally identical to C's
//!   borrowed pointer. The in-place `*_mut` walkers avoid even the copy by
//!   moving the child out and back (`expr_walk_sentinel`).
//!
//! # `raw_expression_tree_walker`
//!
//! The raw-grammar nodes (`SelectStmt`/`A_Expr`/`ColumnRef`/`FuncCall`/
//! `ResTarget`/…) carry their children as `NodePtr`/`PgVec<NodePtr>`, so the raw
//! walker recurses directly over real `Node` children — no wrapping needed.
//!
//! # `*_mut`
//!
//! [`expression_tree_walker_mut`] threads `&mut Node` so `parse_collate`'s
//! assign-collations pass can mutate each child in place, read the assigned
//! state back, and abort/propagate errors. For an `Expr`-struct child it walks a
//! wrapped `Node::Expr` clone and writes the (possibly mutated) result back into
//! the field — the in-place analogue of the read-only clone-wrap.

#![allow(non_snake_case)]

use types_nodes::nodes::ntag;
use types_nodes::nodes::Node;
use types_nodes::primnodes::Expr;

// ===========================================================================
// expression_tree_walker (Node-level) — nodeFuncs.c:2088
// ===========================================================================

/// `expression_tree_walker(node, walker, context)` (nodeFuncs.c) over the full
/// `types_nodes::Node` universe. `walker` returns `true` to abort the whole
/// walk; this function returns `true` iff some `walker` invocation did. Per the
/// C contract the current `node` has already been visited by the caller; this
/// only recurses into its immediate children.
pub fn expression_tree_walker(node: &Node, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    // A scratch `MemoryContext` for the transient `Node::Expr` wrappers built
    // over this node's `Expr` children (see [`node_expr_wrapper`]). C's
    // `expression_tree_walker` passes child *pointers* and never copies; the
    // split Expr/Node model forces an owned `Node::Expr(child)` to satisfy the
    // `FnMut(&Node)` callback, and a bare `.clone()` of an `Aggref`/`SubLink`/…
    // child is a deliberate panic (its arena-pointer children are not shallow-
    // cloneable — see `Expr::clone_in`). We deep-copy each immediate child into
    // this scratch context via the non-panicking `clone_in` path; the wrapper
    // is only read-borrowed by the callback (no consumer takes ownership) so a
    // deep copy is observationally identical to C's pointer, and the context —
    // with every wrapper allocated in it — is freed when this call returns.
    // Re-recursion (a callback re-invoking `expression_tree_walker` on a
    // wrapper) creates its own nested scratch context for the next level, which
    // outlives the wrapper it walks.
    let scratch = mcx::MemoryContext::new("expression_tree_walker scratch");
    let mcx = scratch.mcx();
    // `WALK(child: Option<&NodePtr>)` — a NULL-able `Node *` child.
    macro_rules! walk_opt {
        ($opt:expr) => {
            match $opt {
                Some(n) => walker(&**n),
                None => false,
            }
        };
    }
    // `LIST_WALK(list: &PgVec<NodePtr>)` — iterate `Node *` elements; abort on
    // the first `true`.
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter() {
                if walker(&**e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    // An embedded expression node: recurse into its `Expr` children, each
    // wrapped back into a `Node` for the `FnMut(&Node)` walker. `Node::Expr`
    // spans every `Expr` tag (Var..Aggref), not a single `ntag`, so it is
    // peeled first via the `as_expr` accessor (the dual-homed-tag pattern).
    if let Some(e) = node.as_expr() {
        return walk_expr_children(e, walker, mcx);
    }

    match node.node_tag() {
        // C `case T_List: foreach(temp, (List *) node) WALK(lfirst(temp));` —
        // visit each element. A bare `List` node is a legitimate walker argument
        // (e.g. recordDependencyOnSingleRelExpr is called on a `List *` of
        // expressions).
        ntag::T_List => list_walk!(node.expect_list()),

        // primitive parse/value node types with no expression subnodes
        ntag::T_RangeTblRef
        | ntag::T_SortGroupClause
        | ntag::T_RowMarkClause
        | ntag::T_A_Star
        | ntag::T_ParamRef
        | ntag::T_Integer
        | ntag::T_Float
        | ntag::T_Boolean
        | ntag::T_String
        | ntag::T_BitString => false,

        // do nothing with a sub-Query (mirrors C `case T_Query: break;` inside
        // expression_tree_walker; query_tree_walker is the entry that descends)
        ntag::T_Query => false,

        // C `T_TargetEntry` arm: `WALK(tle->expr)`. The child is wrapped as
        // `Node::Expr` (the `Expr` payload is lifetime-free, so the clone is
        // total — no allocator needed).
        ntag::T_TargetEntry => match node.expect_targetentry().expr.as_deref() {
            Some(e) => walker(&node_expr_wrapper(e, mcx)),
            None => false,
        },

        ntag::T_FromExpr => {
            let from = node.expect_fromexpr();
            list_walk!(from.fromlist) || walk_opt!(from.quals.as_ref())
        }

        ntag::T_JoinExpr => {
            let join = node.expect_joinexpr();
            walk_opt!(join.larg.as_ref())
                || walk_opt!(join.rarg.as_ref())
                || walk_opt!(join.quals.as_ref())
        }

        ntag::T_OnConflictExpr => {
            let oce = node.expect_onconflictexpr();
            list_walk!(oce.arbiterElems)
                || walk_opt!(oce.arbiterWhere.as_ref())
                || list_walk!(oce.onConflictSet)
                || walk_opt!(oce.onConflictWhere.as_ref())
                || list_walk!(oce.exclRelTlist)
        }

        ntag::T_MergeAction => {
            let action = node.expect_mergeaction();
            walk_opt!(action.qual.as_ref()) || list_walk!(action.targetList)
        }

        ntag::T_WindowClause => {
            let wc = node.expect_windowclause();
            list_walk!(wc.partitionClause)
                || list_walk!(wc.orderClause)
                || walk_opt!(wc.startOffset.as_ref())
                || walk_opt!(wc.endOffset.as_ref())
        }

        // C: WALK(ctequery) || WALK(search_clause) || WALK(cycle_clause).
        // search_clause (CTESearchClause) has no expression subnodes (C's
        // expression_tree_walker handles T_CTESearchClause as a no-op-break),
        // so it adds nothing; cycle_clause dispatches to the CTECycleClause arm
        // which recurses cycle_mark_value/cycle_mark_default.
        ntag::T_CommonTableExpr => {
            let cte = node.expect_commontableexpr();
            walk_opt!(cte.ctequery.as_ref()) || walk_opt!(cte.cycle_clause.as_ref())
        }

        // C `T_CTECycleClause`: WALK(cycle_mark_value) || WALK(cycle_mark_default).
        ntag::T_CTECycleClause => {
            let cc = node.expect_ctecycleclause();
            walk_opt!(cc.cycle_mark_value.as_ref()) || walk_opt!(cc.cycle_mark_default.as_ref())
        }

        // C `T_TableFunc`: walk ns_uris, docexpr, rowexpr, colexprs, coldefexprs,
        // colvalexprs, passingvalexprs (all `Expr`-list/`Expr` children, wrapped
        // back into `Node::Expr` for the `FnMut(&Node)` walker; the lists may
        // hold NULL elements).
        ntag::T_TableFunc => walk_table_func(node.expect_tablefunc(), walker, mcx),

        ntag::T_SetOperationStmt => {
            let setop = node.expect_setoperationstmt();
            walk_opt!(setop.larg.as_ref()) || walk_opt!(setop.rarg.as_ref())
        }

        ntag::T_WithCheckOption => walk_opt!(node.expect_withcheckoption().qual.as_ref()),

        ntag::T_RangeTblFunction => walk_opt!(node.expect_rangetblfunction().funcexpr.as_ref()),

        ntag::T_GroupingSet => list_walk!(node.expect_groupingset().content),

        // C `expression_tree_walker` does not descend the planner/raw-grammar
        // *statement* nodes here (the raw walker does). The parse-tree producer
        // nodes that the C `expression_tree_walker` does NOT have an arm for are
        // unreachable through this entry; matching the C default would be
        // elog(ERROR, "unrecognized node type"). For the parse/DDL/raw nodes that
        // are not part of the C expression_tree_walker switch, defer to the raw
        // walker's vocabulary via `_` only after the modeled arms above.
        _ => unrecognized_expression_node(node),
    }
}

/// Recurse into the `Expr` children of an embedded expression node, wrapping
/// each child as `Node::Expr` for the `FnMut(&Node)` walker. This is the
/// `Node`-level bridge over the trimmed [`crate::nodefuncs::expression_tree_walker`]
/// child set (same children, same order), re-expressed so the callback sees a
/// `Node`.
fn walk_expr_children(
    e: &Expr,
    walker: &mut dyn FnMut(&Node) -> bool,
    mcx: mcx::Mcx<'_>,
) -> bool {
    let mut aborted = false;
    // Delegate the *child enumeration* to the canonical Expr-level walker, and
    // for each `&Expr` child re-wrap it as a `Node::Expr` for the `Node` walker.
    // The Expr-level walker has already-visited semantics identical to C (it
    // recurses into children only), so the set/order of children observed here
    // is exactly the C `expression_tree_walker` child set for the corresponding
    // node tag. The wrapper is built via [`node_expr_wrapper`] (deep-copy into
    // `mcx`, never a panicking shallow `.clone()`).
    let mut child_walker = |child: &Expr| -> bool {
        if walker(&node_expr_wrapper(child, mcx)) {
            aborted = true;
            return true;
        }
        false
    };
    crate::nodefuncs::expression_tree_walker(Some(e), &mut child_walker);
    if aborted {
        return true;
    }

    // C `expression_tree_walker` T_SubLink: after `WALK(sublink->testexpr)` it
    // does `return WALK(sublink->subselect)` — i.e. it invokes the walker on the
    // sublink's sub-`Query` node so the callback can recurse into the sub-query
    // (e.g. `query_tree_walker`). The Expr-level walker above only enumerates
    // `&Expr` children, so the `subselect` (a `Query`, not an `Expr`) is dropped
    // there; we wrap it as a `Node::Query` and invoke the `Node` walker here.
    // (lockcmds' `LockViewRecurse_walker` relies on this to lock the tables
    // referenced by a scalar-subquery / `IN (subselect)` in a view definition.)
    if let Expr::SubLink(sublink) = e {
        if let Some(subselect) = sublink.subselect.as_deref() {
            let q = match subselect.clone_in(mcx) {
                Ok(q) => q,
                Err(_) => return false,
            };
            let node = match Node::mk_query(mcx, q) {
                Ok(n) => n,
                Err(_) => return false,
            };
            if walker(&node) {
                return true;
            }
        }
    }
    aborted
}

/// Build the transient `Node::Expr` wrapper an immutable `Node`-level walker
/// hands to its `FnMut(&Node)` callback for an `Expr` child.
///
/// C passes the child *pointer* `(Node *) child` and never copies. The split
/// Expr/Node model cannot borrow an `&Expr` into the owned `Node::Expr(Expr)`
/// arm, and a bare `Expr::clone()` is a deliberate panic for the arena-pointer-
/// carrying variants (`Aggref`/`WindowFunc`/`SubLink`/`SubPlan`/
/// `AlternativeSubPlan` — e.g. the `count(*)` target list's `Aggref`, whose
/// `args` are a `TargetEntry` list with `PgBox` children). So we deep-copy the
/// child into the caller's per-walk scratch `mcx` via the non-panicking
/// `Expr::clone_in` (the same `copyObject`-shape path the planner uses). The
/// wrapper is read-only-borrowed by the callback and freed with the scratch
/// context when the walk level returns, so the deep copy is observationally
/// identical to C's borrowed pointer.
///
/// `clone_in` only fails on allocation failure; an OOM mid-walk is unrecoverable
/// here (the read-only walker contract returns a plain `bool`), so it is a loud
/// `expect` — mirror-PG-and-fail, not a silent skip.
#[inline]
pub fn node_expr_wrapper<'mcx>(e: &Expr, mcx: mcx::Mcx<'mcx>) -> Node<'mcx> {
    // The opaque `Node` is invariant in `'mcx` (the trait-object payload), so the
    // wrapper is tied to `mcx` (the scratch context the deep-copied children live
    // in). The scratch context outliving each callback invocation keeps the
    // wrapped node valid for the duration of the walk.
    Node::mk_expr(
        mcx,
        e.clone_in(mcx)
            .expect("node_expr_wrapper: Expr::clone_in failed (out of memory) during read-only tree walk"),
    )
    .expect("node_expr_wrapper: opaque Node alloc failed (out of memory) during read-only tree walk")
}

/// C `T_TableFunc` arm of `expression_tree_walker` (nodeFuncs.c:2647): WALK
/// each of ns_uris, docexpr, rowexpr, colexprs, coldefexprs, colvalexprs,
/// passingvalexprs in that order. The owned `TableFunc` carries these as
/// `Expr`/`PgVec<PgBox<Expr>>`/`PgVec<Option<PgBox<Expr>>>`; each `&Expr` is
/// re-wrapped as a `Node::Expr` clone for the `FnMut(&Node)` walker (same
/// clone-wrap as [`walk_expr_children`]).
fn walk_table_func(
    tf: &types_nodes::primnodes::TableFunc,
    walker: &mut dyn FnMut(&Node) -> bool,
    mcx: mcx::Mcx<'_>,
) -> bool {
    // `List *` of `Expr *` (no NULL elements).
    fn list(
        l: &Option<mcx::PgVec<'_, mcx::PgBox<'_, Expr>>>,
        walker: &mut dyn FnMut(&Node) -> bool,
        mcx: mcx::Mcx<'_>,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter() {
                if walker(&node_expr_wrapper(&**e, mcx)) {
                    return true;
                }
            }
        }
        false
    }
    // `List *` of `Expr *` allowing NULL elements.
    fn list_opt(
        l: &Option<mcx::PgVec<'_, Option<mcx::PgBox<'_, Expr>>>>,
        walker: &mut dyn FnMut(&Node) -> bool,
        mcx: mcx::Mcx<'_>,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter().flatten() {
                if walker(&node_expr_wrapper(&**e, mcx)) {
                    return true;
                }
            }
        }
        false
    }
    fn opt(
        e: &Option<mcx::PgBox<'_, Expr>>,
        walker: &mut dyn FnMut(&Node) -> bool,
        mcx: mcx::Mcx<'_>,
    ) -> bool {
        match e.as_deref() {
            Some(e) => walker(&node_expr_wrapper(e, mcx)),
            None => false,
        }
    }
    list(&tf.ns_uris, walker, mcx)
        || opt(&tf.docexpr, walker, mcx)
        || opt(&tf.rowexpr, walker, mcx)
        || list_opt(&tf.colexprs, walker, mcx)
        || list_opt(&tf.coldefexprs, walker, mcx)
        || list_opt(&tf.colvalexprs, walker, mcx)
        || list(&tf.passingvalexprs, walker, mcx)
}

/// C default arm of `expression_tree_walker`: `elog(ERROR, "unrecognized node
/// type: %d", nodeTag(node))`. Reached only for a `Node` arm that the C
/// expression-tree walker has no case for (the raw-grammar statement nodes,
/// which are the `raw_expression_tree_walker`'s territory, or DDL/utility nodes).
/// Mirror-PG-and-panic: a faithful loud failure, not a silent skip.
fn unrecognized_expression_node(node: &Node) -> bool {
    panic!(
        "expression_tree_walker: unrecognized node type {} \
         (not part of the C expression_tree_walker switch; raw-grammar statement \
         nodes use raw_expression_tree_walker)",
        node.tag()
    )
}

/// A cheap, allocation-free transient `Expr` used to vacate a `&mut Expr` slot
/// while its real value is moved into a `Node::Expr` wrapper for the in-place
/// mutator. The real value is always written back before this sentinel can be
/// observed; it exists only because `Expr` carries no `Default` and a plain
/// `.clone()` of an `Aggref` child is a deliberate panic (deep-copy is
/// `clone_in`, which needs an allocator the walker does not have).
#[inline]
fn expr_walk_sentinel() -> Expr {
    Expr::CaseTestExpr(types_nodes::primnodes::CaseTestExpr {
        typeId: 0,
        typeMod: -1,
        collation: 0,
    })
}

// ===========================================================================
// expression_tree_walker_mut (Node-level, in-place) — for parse_collate
// ===========================================================================

/// `expression_tree_walker` over `&mut Node` (nodeFuncs.c shape, in-place). Used
/// by `parse_collate`'s `assign_collations_walker`, which walks each child,
/// mutates it in place, reads the assigned collation state back, and aborts on a
/// `walker` `true`. Same child set/order as [`expression_tree_walker`]; threads
/// `&mut Node` instead of `&Node`.
pub fn expression_tree_walker_mut<'mcx>(
    node: &mut Node<'mcx>,
    walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    macro_rules! walk_opt {
        ($opt:expr) => {
            match $opt {
                Some(n) => walker(&mut **n),
                None => false,
            }
        };
    }
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter_mut() {
                if walker(&mut **e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    // An embedded expression node: recurse into its `Expr` children in place.
    // `Node::Expr` spans every `Expr` tag (Var..Aggref), not a single `ntag`, so
    // it is peeled first via the `as_expr_mut` accessor (the dual-homed-tag
    // pattern) before the unambiguous single-tag arms dispatch on `node_tag()`.
    if let Some(e) = node.as_expr_mut() {
        return walk_expr_children_mut(e, walker, mcx);
    }

    match node.node_tag() {
        // C `case T_List: foreach(temp, (List *) node) WALK(lfirst(temp));` —
        // visit each element in place. A bare `List` node is a legitimate walker
        // argument (e.g. the `VALUES` rows reached by `assign_collations`'s
        // in-place walk over `(VALUES ...)` sublists). Mirrors the read-only
        // `expression_tree_walker`'s `T_List` arm.
        ntag::T_List => list_walk!(node.expect_list_mut()),

        ntag::T_RangeTblRef
        | ntag::T_SortGroupClause
        | ntag::T_RowMarkClause
        | ntag::T_A_Star
        | ntag::T_ParamRef
        | ntag::T_Integer
        | ntag::T_Float
        | ntag::T_Boolean
        | ntag::T_String
        | ntag::T_BitString => false,

        ntag::T_Query => false,

        ntag::T_TargetEntry => match node.expect_targetentry_mut().expr.as_deref_mut() {
            Some(e) => {
                // C `T_TargetEntry` arm: `WALK(tle->expr)`, mutating in place.
                // Move the child out (a plain `.clone()` hits `Aggref`'s
                // deliberate panic-`Clone` — e.g. the count(*) tlist), wrap it,
                // walk it, then move the result back. The transient sentinel is
                // always overwritten before it can be observed.
                let owned = core::mem::replace(e, expr_walk_sentinel());
                let mut wrapped = Node::mk_expr(mcx, owned)
                    .expect("expression_tree_walker_mut: opaque Node alloc failed (OOM)");
                let aborted = walker(&mut wrapped);
                if let Some(ne) = wrapped.into_expr() {
                    *e = ne;
                }
                aborted
            }
            None => false,
        },

        ntag::T_FromExpr => {
            let from = node.expect_fromexpr_mut();
            list_walk!(from.fromlist) || walk_opt!(from.quals.as_mut())
        }

        ntag::T_JoinExpr => {
            let join = node.expect_joinexpr_mut();
            walk_opt!(join.larg.as_mut())
                || walk_opt!(join.rarg.as_mut())
                || walk_opt!(join.quals.as_mut())
        }

        ntag::T_OnConflictExpr => {
            let oce = node.expect_onconflictexpr_mut();
            list_walk!(oce.arbiterElems)
                || walk_opt!(oce.arbiterWhere.as_mut())
                || list_walk!(oce.onConflictSet)
                || walk_opt!(oce.onConflictWhere.as_mut())
                || list_walk!(oce.exclRelTlist)
        }

        ntag::T_MergeAction => {
            let action = node.expect_mergeaction_mut();
            walk_opt!(action.qual.as_mut()) || list_walk!(action.targetList)
        }

        ntag::T_WindowClause => {
            let wc = node.expect_windowclause_mut();
            list_walk!(wc.partitionClause)
                || list_walk!(wc.orderClause)
                || walk_opt!(wc.startOffset.as_mut())
                || walk_opt!(wc.endOffset.as_mut())
        }

        ntag::T_CommonTableExpr => {
            let cte = node.expect_commontableexpr_mut();
            walk_opt!(cte.ctequery.as_mut()) || walk_opt!(cte.cycle_clause.as_mut())
        }

        ntag::T_CTECycleClause => {
            let cc = node.expect_ctecycleclause_mut();
            walk_opt!(cc.cycle_mark_value.as_mut()) || walk_opt!(cc.cycle_mark_default.as_mut())
        }

        ntag::T_TableFunc => walk_table_func_mut(node.expect_tablefunc_mut(), walker, mcx),

        ntag::T_SetOperationStmt => {
            let setop = node.expect_setoperationstmt_mut();
            walk_opt!(setop.larg.as_mut()) || walk_opt!(setop.rarg.as_mut())
        }

        ntag::T_WithCheckOption => walk_opt!(node.expect_withcheckoption_mut().qual.as_mut()),

        ntag::T_RangeTblFunction => walk_opt!(node.expect_rangetblfunction_mut().funcexpr.as_mut()),

        ntag::T_GroupingSet => list_walk!(node.expect_groupingset_mut().content),

        _ => unrecognized_expression_node(node),
    }
}

/// In-place analogue of [`walk_table_func`]: walk each `Expr` child as a wrapped
/// `Node::Expr`, writing the (possibly mutated) child back into its field.
fn walk_table_func_mut<'mcx>(
    tf: &mut types_nodes::primnodes::TableFunc,
    walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    fn one<'mcx>(e: &mut Expr, walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool, mcx: mcx::Mcx<'mcx>) -> bool {
        // Move the child out rather than `.clone()` it — an `Aggref` child's
        // `Clone` is a deliberate panic (see `expr_walk_sentinel`).
        let owned = core::mem::replace(e, expr_walk_sentinel());
        let mut wrapped = Node::mk_expr(mcx, owned)
            .expect("walk_table_func_mut: opaque Node alloc failed (OOM)");
        let aborted = walker(&mut wrapped);
        if let Some(ne) = wrapped.into_expr() {
            *e = ne;
        }
        aborted
    }
    fn list<'mcx>(
        l: &mut Option<mcx::PgVec<'_, mcx::PgBox<'_, Expr>>>,
        walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
        mcx: mcx::Mcx<'mcx>,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter_mut() {
                if one(e, walker, mcx) {
                    return true;
                }
            }
        }
        false
    }
    fn list_opt<'mcx>(
        l: &mut Option<mcx::PgVec<'_, Option<mcx::PgBox<'_, Expr>>>>,
        walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
        mcx: mcx::Mcx<'mcx>,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter_mut().flatten() {
                if one(e, walker, mcx) {
                    return true;
                }
            }
        }
        false
    }
    fn opt<'mcx>(
        e: &mut Option<mcx::PgBox<'_, Expr>>,
        walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
        mcx: mcx::Mcx<'mcx>,
    ) -> bool {
        match e.as_deref_mut() {
            Some(e) => one(e, walker, mcx),
            None => false,
        }
    }
    list(&mut tf.ns_uris, walker, mcx)
        || opt(&mut tf.docexpr, walker, mcx)
        || opt(&mut tf.rowexpr, walker, mcx)
        || list_opt(&mut tf.colexprs, walker, mcx)
        || list_opt(&mut tf.coldefexprs, walker, mcx)
        || list_opt(&mut tf.colvalexprs, walker, mcx)
        || list(&mut tf.passingvalexprs, walker, mcx)
}

/// In-place analogue of [`walk_expr_children`]: walk each `Expr` child as a
/// wrapped `Node::Expr`, then write the (possibly mutated) child back into its
/// field. Uses the canonical Expr-level in-place driver
/// [`crate::nodefuncs`]'s child set via a per-child wrap/unwrap.
fn walk_expr_children_mut<'mcx>(
    e: &mut Expr,
    walker: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    let mut aborted = false;
    crate::nodefuncs::for_each_expr_child_mut(e, &mut |child: &mut Expr| {
        if aborted {
            return;
        }
        // Move the child out rather than `.clone()` it — an `Aggref` child's
        // `Clone` is a deliberate panic (see `expr_walk_sentinel`).
        let owned = core::mem::replace(child, expr_walk_sentinel());
        let mut wrapped = Node::mk_expr(mcx, owned)
            .expect("walk_expr_children_mut: opaque Node alloc failed (OOM)");
        if walker(&mut wrapped) {
            aborted = true;
        }
        if let Some(nc) = wrapped.into_expr() {
            *child = nc;
        }
    });
    aborted
}

// ===========================================================================
// raw_expression_tree_walker — nodeFuncs.c:4101
// ===========================================================================

/// `raw_expression_tree_walker(node, walker, context)` (nodeFuncs.c) — the
/// generic recursion over RAW grammar output (pre-parse-analysis): `SelectStmt`,
/// `A_Expr`, `ColumnRef`, `FuncCall`, `ResTarget`, `TypeCast`, etc. These nodes
/// carry their children as `NodePtr`/`PgVec<NodePtr>`, so the recursion descends
/// real `Node` children directly. The current `node` has already been visited;
/// this only recurses into its children.
pub fn raw_expression_tree_walker(node: &Node, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    macro_rules! walk_opt {
        ($opt:expr) => {
            match $opt {
                Some(n) => walker(&**n),
                None => false,
            }
        };
    }
    // raw nodes hold optional/embedded sub-objects in `PgBox<T>` (not NodePtr);
    // those are typed (WindowDef/WithClause/…) and are recursed by re-wrapping
    // into their `Node` arm where the C raw walker descends them.
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter() {
                if walker(&**e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    match node.node_tag() {
        // leaves the raw walker stops at (C: T_SetToDefault/T_CurrentOfExpr/
        // T_Integer/.../T_ParamRef/T_A_Const/T_A_Star → break)
        ntag::T_Integer
        | ntag::T_Float
        | ntag::T_Boolean
        | ntag::T_String
        | ntag::T_BitString
        | ntag::T_ParamRef
        | ntag::T_A_Star
        | ntag::T_RangeVar => false,

        ntag::T_A_Const => false,

        ntag::T_ColumnRef => list_walk!(node.expect_columnref().fields),

        ntag::T_A_Expr => {
            let a = node.expect_a_expr();
            walk_opt!(a.lexpr.as_ref()) || walk_opt!(a.rexpr.as_ref())
        }

        ntag::T_FuncCall => {
            let fc = node.expect_funccall();
            list_walk!(fc.args)
                || list_walk!(fc.agg_order)
                || walk_opt!(fc.agg_filter.as_ref())
                || match fc.over.as_deref() {
                    Some(w) => raw_walk_windowdef(w, walker),
                    None => false,
                }
        }

        ntag::T_A_Indices => {
            let ai = node.expect_a_indices();
            walk_opt!(ai.lidx.as_ref()) || walk_opt!(ai.uidx.as_ref())
        }

        ntag::T_A_Indirection => {
            let ai = node.expect_a_indirection();
            walk_opt!(ai.arg.as_ref()) || list_walk!(ai.indirection)
        }

        ntag::T_A_ArrayExpr => list_walk!(node.expect_a_arrayexpr().elements),

        ntag::T_ResTarget => {
            let rt = node.expect_restarget();
            list_walk!(rt.indirection) || walk_opt!(rt.val.as_ref())
        }

        ntag::T_MultiAssignRef => walk_opt!(node.expect_multiassignref().source.as_ref()),

        ntag::T_TypeCast => walk_opt!(node.expect_typecast().arg.as_ref()),

        ntag::T_CollateClause => walk_opt!(node.expect_collateclause().arg.as_ref()),

        ntag::T_SortBy => walk_opt!(node.expect_sortby().node.as_ref()),

        ntag::T_WindowDef => raw_walk_windowdef(node.expect_windowdef(), walker),

        ntag::T_RangeSubselect => walk_opt!(node.expect_rangesubselect().subquery.as_ref()),

        ntag::T_RangeFunction => list_walk!(node.expect_rangefunction().functions),

        ntag::T_RangeTableSample => {
            let rts = node.expect_rangetablesample();
            walk_opt!(rts.relation.as_ref())
                || list_walk!(rts.args)
                || walk_opt!(rts.repeatable.as_ref())
        }

        ntag::T_TypeName => {
            let tn = node.expect_typename();
            list_walk!(tn.typmods) || list_walk!(tn.arrayBounds)
        }

        ntag::T_ColumnDef => {
            let cd = node.expect_columndef();
            (match cd.typeName.as_deref() {
                Some(tn) => raw_walk_typename(tn, walker),
                None => false,
            }) || walk_opt!(cd.raw_default.as_ref())
            // C also WALKs collClause/constraints, but those are typed
            // sub-objects (CollateClause/Constraint) — collClause is recursed via
            // its `Node` arm below; Constraint is not yet in the Node universe.
        }

        ntag::T_GroupingSet => list_walk!(node.expect_groupingset().content),

        ntag::T_SelectStmt => raw_walk_selectstmt(node.expect_selectstmt(), walker),

        ntag::T_InsertStmt => {
            let i = node.expect_insertstmt();
            list_walk!(i.cols)
                || walk_opt!(i.selectStmt.as_ref())
                || (match i.onConflictClause.as_deref() {
                    Some(c) => raw_walk_onconflictclause(c, walker),
                    None => false,
                })
                || (match i.returningClause.as_deref() {
                    Some(r) => raw_walk_returningclause(r, walker),
                    None => false,
                })
                || (match i.withClause.as_deref() {
                    Some(w) => raw_walk_withclause(w, walker),
                    None => false,
                })
        }

        ntag::T_UpdateStmt => {
            let u = node.expect_updatestmt();
            list_walk!(u.targetList)
                || walk_opt!(u.whereClause.as_ref())
                || list_walk!(u.fromClause)
                || (match u.returningClause.as_deref() {
                    Some(r) => raw_walk_returningclause(r, walker),
                    None => false,
                })
                || (match u.withClause.as_deref() {
                    Some(w) => raw_walk_withclause(w, walker),
                    None => false,
                })
        }

        ntag::T_DeleteStmt => {
            let d = node.expect_deletestmt();
            list_walk!(d.usingClause)
                || walk_opt!(d.whereClause.as_ref())
                || (match d.returningClause.as_deref() {
                    Some(r) => raw_walk_returningclause(r, walker),
                    None => false,
                })
                || (match d.withClause.as_deref() {
                    Some(w) => raw_walk_withclause(w, walker),
                    None => false,
                })
        }

        ntag::T_MergeStmt => {
            let m = node.expect_mergestmt();
            walk_opt!(m.sourceRelation.as_ref())
                || walk_opt!(m.joinCondition.as_ref())
                || list_walk!(m.mergeWhenClauses)
                || (match m.returningClause.as_deref() {
                    Some(r) => raw_walk_returningclause(r, walker),
                    None => false,
                })
                || (match m.withClause.as_deref() {
                    Some(w) => raw_walk_withclause(w, walker),
                    None => false,
                })
        }

        ntag::T_MergeWhenClause => {
            let mwc = node.expect_mergewhenclause();
            walk_opt!(mwc.condition.as_ref())
                || list_walk!(mwc.targetList)
                || list_walk!(mwc.values)
        }

        ntag::T_CommonTableExpr => walk_opt!(node.expect_commontableexpr().ctequery.as_ref()),

        ntag::T_JoinExpr => {
            let join = node.expect_joinexpr();
            walk_opt!(join.larg.as_ref())
                || walk_opt!(join.rarg.as_ref())
                || walk_opt!(join.quals.as_ref())
        }

        ntag::T_FromExpr => {
            let from = node.expect_fromexpr();
            list_walk!(from.fromlist) || walk_opt!(from.quals.as_ref())
        }

        // C `case T_List: foreach(temp, (List *) node) WALK(lfirst(temp))` —
        // a bare List node (e.g. a row of a VALUES list, or an `A_Expr`'s
        // IN-list rexpr) is a legitimate walker argument; visit each element.
        ntag::T_List => list_walk!(node.expect_list()),

        // ---- Expr-deriving nodes the grammar emits (raw form, with raw `Node`
        // children: nodeFuncs.c raw_expression_tree_walker T_SubLink/T_CaseExpr/
        // … arms). Their children are `NodePtr` (raw parse-tree nodes), so they
        // recurse directly through `walk_opt!`/`list_walk!`.
        ntag::T_SubLink => {
            let sl = node.expect_sublink();
            // C: WALK(testexpr) then WALK(subselect); operName is uninteresting.
            walk_opt!(sl.testexpr.as_ref()) || walk_opt!(sl.subselect.as_ref())
        }

        ntag::T_CaseExpr => {
            // C: WALK(arg); foreach WHEN { WALK(when->expr); WALK(when->result) };
            // WALK(defresult). Here `args` is a list of raw `CaseWhen` nodes, so
            // `list_walk!` visits each CaseWhen (handled by its own arm below).
            let ce = node.expect_caseexpr();
            walk_opt!(ce.arg.as_ref()) || list_walk!(ce.args) || walk_opt!(ce.defresult.as_ref())
        }

        ntag::T_CaseWhen => {
            let cw = node.expect_casewhen();
            walk_opt!(cw.expr.as_ref()) || walk_opt!(cw.result.as_ref())
        }

        // C: `return WALK(((RowExpr *) node)->args)` — colnames uninteresting.
        ntag::T_RowExpr => list_walk!(node.expect_rowexpr().args),

        // C: `return WALK(((CoalesceExpr *) node)->args)`.
        ntag::T_CoalesceExpr => list_walk!(node.expect_coalesceexpr().args),

        // C: `return WALK(((MinMaxExpr *) node)->args)`.
        ntag::T_MinMaxExpr => list_walk!(node.expect_minmaxexpr().args),

        // C: `return WALK(((BoolExpr *) node)->args)`.
        ntag::T_BoolExpr => list_walk!(node.expect_boolexpr().args),

        // C: WALK(named_args) then WALK(args); arg_names uninteresting.
        ntag::T_XmlExpr => {
            let x = node.expect_xmlexpr();
            list_walk!(x.named_args) || list_walk!(x.args)
        }

        // C: `return WALK(((GroupingFunc *) node)->args)`.
        ntag::T_GroupingFunc => list_walk!(node.expect_groupingfunc().args),

        // C: `return WALK(((NullTest *) node)->arg)`.
        ntag::T_NullTest => walk_opt!(node.expect_nulltest().arg.as_ref()),

        // C: `return WALK(((BooleanTest *) node)->arg)`.
        ntag::T_BooleanTest => walk_opt!(node.expect_booleantest().arg.as_ref()),

        // C: `return WALK(((NamedArgExpr *) node)->arg)`.
        ntag::T_NamedArgExpr => walk_opt!(node.expect_namedargexpr().arg.as_ref()),

        // C: `return WALK(((CollateExpr *) node)->arg)`.
        ntag::T_CollateExpr => walk_opt!(node.expect_collateexpr().arg.as_ref()),

        // C: WALK(indirection) then WALK(val).
        ntag::T_PLAssignStmt => {
            let p = node.expect_plassignstmt();
            list_walk!(p.indirection) || walk_opt!(p.val.as_ref())
        }

        // C: WALK(options) then WALK(exprs).
        ntag::T_ReturningClause => {
            let r = node.expect_returningclause();
            list_walk!(r.options) || list_walk!(r.exprs)
        }

        // C leaves with no expression subnodes the raw walker stops at
        // (T_SetToDefault/T_CurrentOfExpr/T_SQLValueFunction/T_MergeSupportFunc/
        // T_ReturningOption/T_Alias). T_LockingClause is not an explicit C case
        // but carries no CTE-relevant subnodes (lockedRels are RangeVars by name,
        // deemed uninteresting); treat it as a leaf so a `FOR UPDATE` recursive
        // query reaches its dedicated "not implemented" check instead of erroring
        // in the walker.
        ntag::T_SetToDefault
        | ntag::T_CurrentOfExpr
        | ntag::T_SQLValueFunction
        | ntag::T_MergeSupportFunc
        | ntag::T_ReturningOption
        | ntag::T_LockingClause
        | ntag::T_Alias => false,

        // a sub-Query (post-analysis) embedded in raw output is walked by
        // recursing the central expression walker over it
        ntag::T_Query => false,

        // C raw walker default: elog(ERROR, "unrecognized node type")
        _ => panic!(
            "raw_expression_tree_walker: unrecognized node type {}",
            node.tag()
        ),
    }
}

/// Recurse the children of a `SelectStmt` (the raw walker's `T_SelectStmt` arm),
/// including the recursive set-op `larg`/`rarg` sub-statements. The sub-SELECTs
/// are carried as `PgBox<SelectStmt>` (not `NodePtr`), so they are descended by
/// recursing this helper directly rather than re-wrapping into a fresh `Node`
/// (the read-only walk only borrows; this preserves the C behavior of recursing
/// into `stmt->larg`/`stmt->rarg`).
fn raw_walk_selectstmt(
    s: &types_nodes::rawnodes::SelectStmt,
    walker: &mut dyn FnMut(&Node) -> bool,
) -> bool {
    macro_rules! lw {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter() {
                if walker(&**e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }
    macro_rules! wo {
        ($opt:expr) => {
            match $opt {
                Some(n) => walker(&**n),
                None => false,
            }
        };
    }
    lw!(s.distinctClause)
        || wo!(s.intoClause.as_ref())
        || lw!(s.targetList)
        || lw!(s.fromClause)
        || wo!(s.whereClause.as_ref())
        || lw!(s.groupClause)
        || wo!(s.havingClause.as_ref())
        || lw!(s.windowClause)
        || lw!(s.valuesLists)
        || lw!(s.sortClause)
        || wo!(s.limitOffset.as_ref())
        || wo!(s.limitCount.as_ref())
        || lw!(s.lockingClause)
        || (match s.withClause.as_deref() {
            Some(w) => raw_walk_withclause(w, walker),
            None => false,
        })
        || (match s.larg.as_deref() {
            Some(l) => raw_walk_selectstmt(l, walker),
            None => false,
        })
        || (match s.rarg.as_deref() {
            Some(r) => raw_walk_selectstmt(r, walker),
            None => false,
        })
}

fn raw_walk_windowdef(wd: &types_nodes::rawnodes::WindowDef, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    for e in wd.partitionClause.iter() {
        if walker(&**e) {
            return true;
        }
    }
    for e in wd.orderClause.iter() {
        if walker(&**e) {
            return true;
        }
    }
    if let Some(s) = wd.startOffset.as_ref() {
        if walker(&**s) {
            return true;
        }
    }
    if let Some(s) = wd.endOffset.as_ref() {
        if walker(&**s) {
            return true;
        }
    }
    false
}

fn raw_walk_withclause(wc: &types_nodes::rawnodes::WithClause, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    for e in wc.ctes.iter() {
        if walker(&**e) {
            return true;
        }
    }
    false
}

fn raw_walk_onconflictclause(
    occ: &types_nodes::rawnodes::OnConflictClause,
    walker: &mut dyn FnMut(&Node) -> bool,
) -> bool {
    if let Some(infer) = occ.infer.as_deref() {
        for e in infer.indexElems.iter() {
            if walker(&**e) {
                return true;
            }
        }
        if let Some(w) = infer.whereClause.as_ref() {
            if walker(&**w) {
                return true;
            }
        }
    }
    for e in occ.targetList.iter() {
        if walker(&**e) {
            return true;
        }
    }
    if let Some(w) = occ.whereClause.as_ref() {
        if walker(&**w) {
            return true;
        }
    }
    false
}

fn raw_walk_returningclause(
    rc: &types_nodes::rawnodes::ReturningClause,
    walker: &mut dyn FnMut(&Node) -> bool,
) -> bool {
    for e in rc.exprs.iter() {
        if walker(&**e) {
            return true;
        }
    }
    false
}

fn raw_walk_typename(tn: &types_nodes::rawnodes::TypeName, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    for e in tn.typmods.iter() {
        if walker(&**e) {
            return true;
        }
    }
    for e in tn.arrayBounds.iter() {
        if walker(&**e) {
            return true;
        }
    }
    false
}

// ===========================================================================
// Statement-level drivers — nodeFuncs.c query_tree_walker family
// ===========================================================================

/// QTW flag: ignore RTEs that are subqueries (`query_tree_walker` skip mask).
pub const QTW_IGNORE_RT_SUBQUERIES: i32 = 0x02;
/// QTW flag: ignore JOIN alias var lists.
pub const QTW_IGNORE_JOINALIASES: i32 = 0x04;
/// QTW flag: ignore expressions in range table entirely.
pub const QTW_IGNORE_RANGE_TABLE: i32 = 0x08;
/// QTW flag: examine RTE column aliases.
pub const QTW_EXAMINE_RTES_BEFORE: i32 = 0x10;
/// QTW flag: examine RTEs after their contents.
pub const QTW_EXAMINE_RTES_AFTER: i32 = 0x20;
/// QTW flag: do not descend into sublink subqueries.
pub const QTW_IGNORE_CTE_SUBQUERIES: i32 = 0x40;
/// QTW flag: ignore GROUP-clause RTEs.
pub const QTW_EXAMINE_SORTGROUP: i32 = 0x80;
/// QTW flag: ignore the RTE_GROUP groupexprs list (nodeFuncs.h).
pub const QTW_IGNORE_GROUPEXPRS: i32 = 0x100;

/// `query_tree_walker(query, walker, context, flags)` (nodeFuncs.c) — apply
/// `walker` to all the expression trees hanging off a `Query`, then recurse into
/// sub-queries via the range table per `flags`. Returns `true` on abort.
pub fn query_tree_walker(
    query: &types_nodes::copy_query::Query,
    walker: &mut dyn FnMut(&Node) -> bool,
    flags: i32,
) -> bool {
    // Per-walk scratch context for the transient `Node::Expr` wrappers built
    // over this `Query`'s `Expr`-typed fields (see [`node_expr_wrapper`] and the
    // matching rationale on `expression_tree_walker`).
    let scratch = mcx::MemoryContext::new("query_tree_walker scratch");
    let mcx = scratch.mcx();
    macro_rules! walk_opt {
        ($opt:expr) => {
            match $opt {
                Some(n) => walker(&**n),
                None => false,
            }
        };
    }
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter() {
                if walker(&**e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    // Expression-only `Query` fields (havingQual/limitOffset/limitCount/
    // mergeJoinCondition) are concretely typed `Option<PgBox<Expr>>`; like a
    // TargetEntry's expr they cannot be re-wrapped into a `Node` without an
    // allocator, so WALK the expression directly as a `Node::Expr` wrapper (the
    // same child the C reaches), deep-copied into `mcx` via `node_expr_wrapper`
    // rather than a panicking shallow `.clone()`.
    macro_rules! walk_opt_expr {
        ($opt:expr) => {
            match $opt {
                Some(e) => walker(&node_expr_wrapper(&**e, mcx)),
                None => false,
            }
        };
    }

    // targetList / returningList are `Vec<TargetEntry>` (typed), wrapped per-elem.
    if walk_targetentry_list(&query.targetList, walker, mcx) {
        return true;
    }
    if let Some(oc) = query.onConflict.as_deref() {
        if walk_onconflict_expr(oc, walker) {
            return true;
        }
    }
    if walk_targetentry_list(&query.returningList, walker, mcx) {
        return true;
    }
    if let Some(jt) = query.jointree.as_deref() {
        if walk_fromexpr(jt, walker) {
            return true;
        }
    }
    if list_walk!(query.mergeActionList) {
        return true;
    }
    if walk_opt_expr!(query.mergeJoinCondition.as_ref()) {
        return true;
    }
    if list_walk!(query.windowClause) {
        return true;
    }
    if walk_opt_expr!(query.havingQual.as_ref()) {
        return true;
    }
    if walk_opt_expr!(query.limitOffset.as_ref()) {
        return true;
    }
    if walk_opt_expr!(query.limitCount.as_ref()) {
        return true;
    }
    if list_walk!(query.withCheckOptions) {
        return true;
    }
    if walk_opt!(query.setOperations.as_ref()) {
        return true;
    }
    // C also walks groupClause/distinctClause/sortClause SortGroupClause lists
    // only under QTW_EXAMINE_SORTGROUP; they are primitives (no subnodes) so a
    // plain `false`/visit-only otherwise. The cteList walk descends into each
    // CommonTableExpr (and thence its ctequery), but is suppressed under
    // QTW_IGNORE_CTE_SUBQUERIES — the leg plancache.c's ScanQueryForLocks uses
    // (it has already locked the CTE subqueries itself) via QTW_IGNORE_RC_SUBQUERIES.
    if flags & QTW_IGNORE_CTE_SUBQUERIES == 0 {
        if list_walk!(query.cteList) {
            return true;
        }
    }
    // Finally the range table.
    if flags & QTW_IGNORE_RANGE_TABLE == 0 {
        if range_table_walker(&query.rtable, walker, flags) {
            return true;
        }
    }
    false
}

/// `range_table_walker(rtable, walker, context, flags)` (nodeFuncs.c).
pub fn range_table_walker(
    rtable: &[types_nodes::parsenodes::RangeTblEntry],
    walker: &mut dyn FnMut(&Node) -> bool,
    flags: i32,
) -> bool {
    for rte in rtable {
        if range_table_entry_walker(rte, walker, flags) {
            return true;
        }
    }
    false
}

/// `range_table_entry_walker(rte, walker, context, flags)` (nodeFuncs.c) — walk
/// the expression trees of a single RTE, descending into its subquery per
/// `flags`.
pub fn range_table_entry_walker(
    rte: &types_nodes::parsenodes::RangeTblEntry,
    walker: &mut dyn FnMut(&Node) -> bool,
    flags: i32,
) -> bool {
    use types_nodes::parsenodes::RTEKind;

    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter() {
                if walker(&**e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            if let Some(ts) = rte.tablesample.as_ref() {
                if walker(&**ts) {
                    return true;
                }
            }
        }
        RTEKind::RTE_SUBQUERY => {
            if flags & QTW_IGNORE_RT_SUBQUERIES == 0 {
                if let Some(sub) = rte.subquery.as_deref() {
                    // C: `WALK(rte->subquery)` — `WALK(n)` is `walker((Node *) n,
                    // context)`, i.e. it invokes the user *callback* on the
                    // sub-`Query` node (which then decides whether to recurse via
                    // `query_tree_walker`). Calling `query_tree_walker` directly
                    // here would descend the subquery's children but skip
                    // invoking the callback ON the `Query` node itself — and a
                    // walker that does per-`Query` work (e.g. lockcmds'
                    // `LockViewRecurse_walker`, which locks each RTE relation when
                    // it is handed a `Query`) would never see a FROM-subquery's
                    // range table. So wrap the subquery as a `Node::Query` and
                    // invoke the callback, exactly like C.
                    let scratch = mcx::MemoryContext::new("range_table_entry_walker subquery");
                    let m = scratch.mcx();
                    let q = match sub.clone_in(m) {
                        Ok(q) => q,
                        Err(_) => return false,
                    };
                    let node = match Node::mk_query(m, q) {
                        Ok(n) => n,
                        Err(_) => return false,
                    };
                    if walker(&node) {
                        return true;
                    }
                }
            }
        }
        RTEKind::RTE_JOIN => {
            if flags & QTW_IGNORE_JOINALIASES == 0 {
                if list_walk!(rte.joinaliasvars) {
                    return true;
                }
            }
        }
        RTEKind::RTE_FUNCTION => {
            if list_walk!(rte.functions) {
                return true;
            }
        }
        RTEKind::RTE_TABLEFUNC => {
            if let Some(tf) = rte.tablefunc.as_ref() {
                if walker(&**tf) {
                    return true;
                }
            }
        }
        RTEKind::RTE_VALUES => {
            if list_walk!(rte.values_lists) {
                return true;
            }
        }
        RTEKind::RTE_GROUP => {
            if flags & QTW_IGNORE_GROUPEXPRS == 0 && list_walk!(rte.groupexprs) {
                return true;
            }
        }
        _ => {}
    }
    if list_walk!(rte.securityQuals) {
        return true;
    }
    false
}

/// `query_or_expression_tree_walker(node, walker, context, flags)` (nodeFuncs.c)
/// — dispatch on whether `node` is a `Query` (descend via query_tree_walker) or
/// a bare expression (visit + descend via expression_tree_walker).
pub fn query_or_expression_tree_walker(
    node: &Node,
    walker: &mut dyn FnMut(&Node) -> bool,
    flags: i32,
) -> bool {
    if node.is_query() {
        query_tree_walker(node.expect_query(), walker, flags)
    } else {
        walker(node)
    }
}

// ===========================================================================
// query_tree_mutator family — nodeFuncs.c (in-place owned-tree variant)
// ===========================================================================

/// `query_tree_mutator(query, mutator, context, flags)` (nodeFuncs.c), expressed
/// as the in-place owned-tree variant: apply `mutator` to every expression child
/// of the `Query` (and, per `flags`, its range table) so each can be mutated in
/// place. Returns `true` on abort (the mutator's `true`). The C "make a modified
/// copy" is realized as in-place mutation through `&mut Node` children, the
/// owned-tree convention used across this repo's `*_mut` walkers.
pub fn query_tree_mutator<'mcx>(
    query: &mut types_nodes::copy_query::Query<'mcx>,
    mutator: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    flags: i32,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    macro_rules! walk_opt {
        ($opt:expr) => {
            match $opt {
                Some(n) => mutator(&mut **n),
                None => false,
            }
        };
    }
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter_mut() {
                if mutator(&mut **e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }

    // Expression-only `Query` fields are concretely typed `Option<PgBox<Expr>>`;
    // MUTATE the expression through a `Node::Expr` wrapper and write back, exactly
    // as `mutate_targetentry_list` does for a TargetEntry's expr.
    macro_rules! mutate_opt_expr {
        ($opt:expr) => {
            match $opt {
                Some(e) => {
                    // Move the child out (a plain `.clone()` hits `Aggref`'s
                    // deliberate panic-`Clone`), wrap it, mutate, then move the
                    // (possibly mutated) result back — the in-place no-copy
                    // analogue used throughout the `*_mut` walkers.
                    let owned = core::mem::replace(&mut **e, expr_walk_sentinel());
                    let mut wrapped = Node::mk_expr(mcx, owned)
                        .expect("query_tree_mutator: opaque Node alloc failed (OOM)");
                    let aborted = mutator(&mut wrapped);
                    if let Some(ne) = wrapped.into_expr() {
                        **e = ne;
                    }
                    aborted
                }
                None => false,
            }
        };
    }

    if mutate_targetentry_list(&mut query.targetList, mutator, mcx) {
        return true;
    }
    if mutate_targetentry_list(&mut query.returningList, mutator, mcx) {
        return true;
    }
    if let Some(jt) = query.jointree.as_deref_mut() {
        if mutate_fromexpr(jt, mutator) {
            return true;
        }
    }
    if list_walk!(query.mergeActionList) {
        return true;
    }
    if mutate_opt_expr!(query.mergeJoinCondition.as_mut()) {
        return true;
    }
    if list_walk!(query.windowClause) {
        return true;
    }
    if mutate_opt_expr!(query.havingQual.as_mut()) {
        return true;
    }
    if mutate_opt_expr!(query.limitOffset.as_mut()) {
        return true;
    }
    if mutate_opt_expr!(query.limitCount.as_mut()) {
        return true;
    }
    if list_walk!(query.withCheckOptions) {
        return true;
    }
    if walk_opt!(query.setOperations.as_mut()) {
        return true;
    }
    if list_walk!(query.cteList) {
        return true;
    }
    if flags & QTW_IGNORE_RANGE_TABLE == 0 {
        if range_table_mutator(&mut query.rtable, mutator, flags, mcx) {
            return true;
        }
    }
    false
}

/// `range_table_mutator(rtable, mutator, context, flags)` (nodeFuncs.c) —
/// in-place owned-tree variant.
pub fn range_table_mutator<'mcx>(
    rtable: &mut [types_nodes::parsenodes::RangeTblEntry<'mcx>],
    mutator: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    flags: i32,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    use types_nodes::parsenodes::RTEKind;
    macro_rules! list_walk {
        ($list:expr) => {{
            let mut aborted = false;
            for e in $list.iter_mut() {
                if mutator(&mut **e) {
                    aborted = true;
                    break;
                }
            }
            aborted
        }};
    }
    for rte in rtable.iter_mut() {
        match rte.rtekind {
            RTEKind::RTE_SUBQUERY => {
                if flags & QTW_IGNORE_RT_SUBQUERIES == 0 {
                    if let Some(sub) = rte.subquery.as_deref_mut() {
                        if query_tree_mutator(sub, mutator, flags, mcx) {
                            return true;
                        }
                    }
                }
            }
            RTEKind::RTE_JOIN => {
                if flags & QTW_IGNORE_JOINALIASES == 0 {
                    if list_walk!(rte.joinaliasvars) {
                        return true;
                    }
                }
            }
            RTEKind::RTE_FUNCTION => {
                if list_walk!(rte.functions) {
                    return true;
                }
            }
            RTEKind::RTE_TABLEFUNC => {
                if let Some(tf) = rte.tablefunc.as_mut() {
                    if mutator(&mut **tf) {
                        return true;
                    }
                }
            }
            RTEKind::RTE_VALUES => {
                if list_walk!(rte.values_lists) {
                    return true;
                }
            }
            RTEKind::RTE_GROUP => {
                if flags & QTW_IGNORE_GROUPEXPRS == 0 && list_walk!(rte.groupexprs) {
                    return true;
                }
            }
            RTEKind::RTE_RELATION => {
                if let Some(ts) = rte.tablesample.as_mut() {
                    if mutator(&mut **ts) {
                        return true;
                    }
                }
            }
            _ => {}
        }
        if list_walk!(rte.securityQuals) {
            return true;
        }
    }
    false
}

/// `query_or_expression_tree_mutator(node, mutator, context, flags)`
/// (nodeFuncs.c) — in-place owned-tree variant.
pub fn query_or_expression_tree_mutator<'mcx>(
    node: &mut Node<'mcx>,
    mutator: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    flags: i32,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    match node.node_tag() {
        ntag::T_Query => query_tree_mutator(node.expect_query_mut(), mutator, flags, mcx),
        _ => mutator(node),
    }
}

fn mutate_targetentry_list<'mcx>(
    list: &mut [types_nodes::primnodes::TargetEntry],
    mutator: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
    mcx: mcx::Mcx<'mcx>,
) -> bool {
    for te in list.iter_mut() {
        if let Some(e) = te.expr.as_deref_mut() {
            // Move the child out rather than `.clone()` it — an `Aggref` child's
            // `Clone` is a deliberate panic (see `expr_walk_sentinel`).
            let owned = core::mem::replace(e, expr_walk_sentinel());
            let mut wrapped = Node::mk_expr(mcx, owned)
                .expect("mutate_targetentry_list: opaque Node alloc failed (OOM)");
            let aborted = mutator(&mut wrapped);
            if let Some(ne) = wrapped.into_expr() {
                *e = ne;
            }
            if aborted {
                return true;
            }
        }
    }
    false
}

fn mutate_fromexpr<'mcx>(
    from: &mut types_nodes::rawnodes::FromExpr<'mcx>,
    mutator: &mut dyn FnMut(&mut Node<'mcx>) -> bool,
) -> bool {
    for e in from.fromlist.iter_mut() {
        if mutator(&mut **e) {
            return true;
        }
    }
    if let Some(q) = from.quals.as_mut() {
        if mutator(&mut **q) {
            return true;
        }
    }
    false
}

// ===========================================================================
// planstate_tree_walker — nodeFuncs.c:4316
// ===========================================================================

/// `planstate_tree_walker(planstate, walker, context)` (nodeFuncs.c) — recurse
/// into the child `PlanState` nodes of an executor state node (init/sub-plans
/// via `planstate_walk_subplans`, outer/inner state, and the per-node member
/// lists via `planstate_walk_members`). The child enumeration is owned by
/// [`types_nodes::PlanStateNode::planstate_tree_walker_children_mut`] (the
/// single place that knows each variant's child layout); this driver invokes
/// `walker` on each child in walk order. Threads `&mut` (the only child accessor
/// the model exposes, matching the parallel-executor estimate/init walks).
/// Returns `true` on abort.
pub fn planstate_tree_walker(
    planstate: &mut types_nodes::PlanStateNode,
    walker: &mut dyn FnMut(&mut types_nodes::PlanStateNode) -> bool,
) -> bool {
    for child in planstate.planstate_tree_walker_children_mut() {
        if walker(child) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_nodes::primnodes::{Expr, OpExpr, Var};
    use types_nodes::rawnodes::{FromExpr, JoinExpr};
    use types_nodes::jointype::JoinType;

    fn op_with_two_vars() -> Expr {
        Expr::OpExpr(OpExpr {
            args: vec![Expr::Var(Var::default()), Expr::Var(Var::default())],
            ..OpExpr::default()
        })
    }

    #[test]
    fn node_walker_recurses_expr_children() {
        // OpExpr with two Var children: walker should see exactly 2 children.
        let node = Node::Expr(op_with_two_vars());
        let mut count = 0;
        let aborted = expression_tree_walker(&node, &mut |c: &Node| {
            assert!(c.is_var());
            count += 1;
            false
        });
        assert!(!aborted);
        assert_eq!(count, 2);
    }

    #[test]
    fn node_walker_abort_stops_early() {
        let node = Node::Expr(op_with_two_vars());
        let mut count = 0;
        let aborted = expression_tree_walker(&node, &mut |_c: &Node| {
            count += 1;
            true // abort on the first child
        });
        assert!(aborted);
        assert_eq!(count, 1);
    }

    #[test]
    fn value_node_tags_match_nodetags_h() {
        use types_nodes::nodes::{T_BitString, T_Boolean, T_Float, T_Integer, T_String};
        use types_nodes::value::{BitString, Boolean, Float, Integer, StringNode};
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        assert_eq!(Node::mk_integer(mcx, Integer { ival: 7 }).tag(), T_Integer);
        assert_eq!(Node::mk_boolean(mcx, Boolean { boolval: true }).tag(), T_Boolean);
        assert_eq!(
            Node::mk_float(mcx, Float {
                fval: mcx::PgString::from_str_in("1.5", mcx).unwrap()
            })
            .tag(),
            T_Float
        );
        assert_eq!(
            Node::mk_string(mcx, StringNode {
                sval: mcx::PgString::from_str_in("x", mcx).unwrap()
            })
            .tag(),
            T_String
        );
        assert_eq!(
            Node::mk_bit_string(mcx, BitString {
                bsval: mcx::PgString::from_str_in("b101", mcx).unwrap()
            })
            .tag(),
            T_BitString
        );
        // value nodes are leaves: no children walked
        let node = Node::mk_integer(mcx, Integer { ival: 7 });
        assert!(!expression_tree_walker(&node, &mut |_c| panic!("leaf has no children")));
    }

    #[test]
    fn node_walker_descends_nodeptr_children() {
        // FromExpr { fromlist: [JoinExpr{quals: OpExpr(Var,Var)}], quals: Var }
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();

        let join = Node::mk_join_expr(mcx, JoinExpr {
            jointype: JoinType::JOIN_INNER,
            isNatural: false,
            larg: None,
            rarg: None,
            usingClause: mcx::PgVec::new_in(mcx),
            join_using_alias: None,
            quals: Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, op_with_two_vars())).unwrap()),
            alias: None,
            rtindex: 0,
        });

        let mut fromlist = mcx::PgVec::new_in(mcx);
        fromlist.push(mcx::alloc_in(mcx, join).unwrap());
        let from = FromExpr {
            fromlist,
            quals: Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, Expr::Var(Var::default()))).unwrap()),
        };
        let node = Node::mk_from_expr(mcx, from);

        // Top-level children: the JoinExpr and the quals Var.
        let mut tags = Vec::new();
        expression_tree_walker(&node, &mut |c: &Node| {
            tags.push(c.tag());
            false
        });
        assert_eq!(tags.len(), 2);
        assert!(node.as_var().is_none());
    }

    /// Build a `count(*)`-shaped `Aggref` (no args, no filter). Its derived
    /// `Clone` is a deliberate panic-stub, so any read-only walk that wraps it
    /// via a bare `.clone()` aborts; this is the exact node the `count(*)`
    /// target list carries.
    fn count_star_aggref() -> Expr {
        use types_nodes::nodeagg::AggSplit;
        use types_nodes::primnodes::Aggref;
        Expr::Aggref(Aggref {
            aggfnoid: 2803, // count(*)
            aggtype: 20,
            aggcollid: 0,
            inputcollid: 0,
            aggtranstype: 20,
            aggargtypes: Vec::new(),
            aggdirectargs: Vec::new(),
            args: Vec::new(),
            aggorder: Vec::new(),
            aggdistinct: Vec::new(),
            aggfilter: None,
            aggstar: true,
            aggvariadic: false,
            aggkind: b'n' as i8,
            aggpresorted: false,
            agglevelsup: 0,
            aggsplit: AggSplit::default(),
            aggno: -1,
            aggtransno: -1,
            location: -1,
        })
    }

    /// Regression for the immutable-walker clone divergence: an immutable walk
    /// over a target list carrying an `Aggref` must NOT panic. Previously
    /// `walk_targetentry_list` wrapped the `Aggref` via `Node::Expr(e.clone())`,
    /// hitting `Aggref`'s deliberate panic-`Clone` (the `count(*)` wall). The
    /// fix deep-copies the child into a per-walk scratch context via
    /// `node_expr_wrapper`/`Expr::clone_in` instead.
    #[test]
    fn immutable_targetentry_list_walk_over_aggref_does_not_panic() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();

        let mut tlist: Vec<types_nodes::primnodes::TargetEntry> = Vec::new();
        tlist.push(types_nodes::primnodes::TargetEntry {
            expr: Some(mcx::alloc_in(mcx, count_star_aggref()).unwrap()),
            resno: 1,
            ..Default::default()
        });

        // Visit the tlist; the callback inspects the wrapped node (read-only)
        // and re-recurses, exactly as e.g. `rangeTableEntry_used_walker` does.
        let mut seen_aggref = false;
        let aborted = walk_targetentry_list(&tlist, &mut |c: &Node| {
            if c.is_aggref() {
                seen_aggref = true;
            }
            // Re-recurse into children (no-op for the arg-less count(*) Aggref),
            // exercising the nested-scratch-context path.
            expression_tree_walker(c, &mut |_g: &Node| false)
        }, mcx);

        assert!(!aborted);
        assert!(seen_aggref, "walker should observe the Aggref target");
    }

    /// The same divergence on the `query_tree_walker` entry (`havingQual` and
    /// the `targetList`), driven through a `Query` carrying an `Aggref` target.
    #[test]
    fn immutable_query_tree_walk_over_aggref_does_not_panic() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();

        let mut query = types_nodes::copy_query::Query::new(mcx);
        query.targetList.push(types_nodes::primnodes::TargetEntry {
            expr: Some(mcx::alloc_in(mcx, count_star_aggref()).unwrap()),
            resno: 1,
            ..Default::default()
        });
        // havingQual is an `Option<PgBox<Expr>>` Expr field walked via
        // `walk_opt_expr!` — also previously clone-panicked on an Aggref.
        query.havingQual = Some(mcx::alloc_in(mcx, count_star_aggref()).unwrap());

        let mut count = 0;
        let aborted = query_tree_walker(&query, &mut |c: &Node| {
            if c.is_aggref() {
                count += 1;
            }
            false
        }, 0);
        assert!(!aborted);
        assert_eq!(count, 2, "both the tlist Aggref and havingQual Aggref are visited");
    }

    #[test]
    fn mut_walker_writes_child_back() {
        // Mutate each Var child's varno via the in-place Node walker.
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut node = Node::Expr(op_with_two_vars());
        let aborted = expression_tree_walker_mut(&mut node, &mut |c: &mut Node| {
            if let Some(v) = c.as_var_mut() {
                v.varno = 42;
            }
            false
        }, mcx);
        assert!(!aborted);
        if let Some(o) = node.as_opexpr() {
            for a in &o.args {
                if let Expr::Var(v) = a {
                    assert_eq!(v.varno, 42);
                } else {
                    panic!("expected Var");
                }
            }
        } else {
            panic!("expected OpExpr");
        }
    }
}

// --- typed-list / typed-child helpers for the statement walkers ------------

fn walk_targetentry_list(
    list: &[types_nodes::primnodes::TargetEntry],
    walker: &mut dyn FnMut(&Node) -> bool,
    mcx: mcx::Mcx<'_>,
) -> bool {
    // C visits `(Node *) tle`, whose `T_TargetEntry` arm WALKs `tle->expr`. In
    // the split `PgBox`-based model a `TargetEntry` cannot be re-wrapped into a
    // `Node` without an allocator (its `expr` is a `PgBox`), so we WALK the
    // expression directly as a `Node::Expr` wrapper — the same child the
    // TargetEntry arm would reach. The wrapper is deep-copied into `mcx` via
    // [`node_expr_wrapper`] (never a panicking shallow `.clone()`, which would
    // abort on an `Aggref` target — e.g. the `count(*)` tlist).
    for te in list {
        if let Some(e) = te.expr.as_deref() {
            if walker(&node_expr_wrapper(e, mcx)) {
                return true;
            }
        }
    }
    false
}

fn walk_fromexpr(
    from: &types_nodes::rawnodes::FromExpr,
    walker: &mut dyn FnMut(&Node) -> bool,
) -> bool {
    for e in from.fromlist.iter() {
        if walker(&**e) {
            return true;
        }
    }
    if let Some(q) = from.quals.as_ref() {
        if walker(&**q) {
            return true;
        }
    }
    false
}

fn walk_onconflict_expr(
    oce: &types_nodes::rawnodes::OnConflictExpr,
    walker: &mut dyn FnMut(&Node) -> bool,
) -> bool {
    for e in oce.arbiterElems.iter() {
        if walker(&**e) {
            return true;
        }
    }
    if let Some(w) = oce.arbiterWhere.as_ref() {
        if walker(&**w) {
            return true;
        }
    }
    for e in oce.onConflictSet.iter() {
        if walker(&**e) {
            return true;
        }
    }
    if let Some(w) = oce.onConflictWhere.as_ref() {
        if walker(&**w) {
            return true;
        }
    }
    for e in oce.exclRelTlist.iter() {
        if walker(&**e) {
            return true;
        }
    }
    false
}

