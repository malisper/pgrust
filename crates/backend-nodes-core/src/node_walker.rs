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
//!   `PgBox<Expr>`) — wrapped on the fly into `Node::Expr(child.clone())` so the
//!   `FnMut(&Node)` walker observes it. This clone-wrap mirrors the C walker
//!   visiting the in-place `(Node *) child`: the walker only borrows the node,
//!   so the clone is observationally identical (the same approach the
//!   src-idiomatic `backend-nodes-nodefuncs` walker uses for its typed-child
//!   arms). The `Expr` subtree is lifetime-free, so the clone is cheap and
//!   total.
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

    match node {
        // An embedded expression node: recurse into its `Expr` children, each
        // wrapped back into a `Node` for the `FnMut(&Node)` walker.
        Node::Expr(e) => walk_expr_children(e, walker),

        // C `case T_List: foreach(temp, (List *) node) WALK(lfirst(temp));` —
        // visit each element. A bare `List` node is a legitimate walker argument
        // (e.g. recordDependencyOnSingleRelExpr is called on a `List *` of
        // expressions).
        Node::List(items) => list_walk!(items),

        // primitive parse/value node types with no expression subnodes
        Node::RangeTblRef(_)
        | Node::SortGroupClause(_)
        | Node::RowMarkClause(_)
        | Node::A_Star(_)
        | Node::ParamRef(_)
        | Node::Integer(_)
        | Node::Float(_)
        | Node::Boolean(_)
        | Node::String(_)
        | Node::BitString(_) => false,

        // do nothing with a sub-Query (mirrors C `case T_Query: break;` inside
        // expression_tree_walker; query_tree_walker is the entry that descends)
        Node::Query(_) => false,

        // C `T_TargetEntry` arm: `WALK(tle->expr)`. The child is wrapped as
        // `Node::Expr` (the `Expr` payload is lifetime-free, so the clone is
        // total — no allocator needed).
        Node::TargetEntry(te) => match te.expr.as_deref() {
            Some(e) => walker(&Node::Expr(e.clone())),
            None => false,
        },

        Node::FromExpr(from) => list_walk!(from.fromlist) || walk_opt!(from.quals.as_ref()),

        Node::JoinExpr(join) => {
            walk_opt!(join.larg.as_ref())
                || walk_opt!(join.rarg.as_ref())
                || walk_opt!(join.quals.as_ref())
        }

        Node::OnConflictExpr(oce) => {
            list_walk!(oce.arbiterElems)
                || walk_opt!(oce.arbiterWhere.as_ref())
                || list_walk!(oce.onConflictSet)
                || walk_opt!(oce.onConflictWhere.as_ref())
                || list_walk!(oce.exclRelTlist)
        }

        Node::MergeAction(action) => {
            walk_opt!(action.qual.as_ref()) || list_walk!(action.targetList)
        }

        Node::WindowClause(wc) => {
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
        Node::CommonTableExpr(cte) => {
            walk_opt!(cte.ctequery.as_ref()) || walk_opt!(cte.cycle_clause.as_ref())
        }

        // C `T_CTECycleClause`: WALK(cycle_mark_value) || WALK(cycle_mark_default).
        Node::CTECycleClause(cc) => {
            walk_opt!(cc.cycle_mark_value.as_ref()) || walk_opt!(cc.cycle_mark_default.as_ref())
        }

        // C `T_TableFunc`: walk ns_uris, docexpr, rowexpr, colexprs, coldefexprs,
        // colvalexprs, passingvalexprs (all `Expr`-list/`Expr` children, wrapped
        // back into `Node::Expr` for the `FnMut(&Node)` walker; the lists may
        // hold NULL elements).
        Node::TableFunc(tf) => walk_table_func(tf, walker),

        Node::SetOperationStmt(setop) => {
            walk_opt!(setop.larg.as_ref()) || walk_opt!(setop.rarg.as_ref())
        }

        Node::WithCheckOption(wco) => walk_opt!(wco.qual.as_ref()),

        Node::RangeTblFunction(rtf) => walk_opt!(rtf.funcexpr.as_ref()),

        Node::GroupingSet(gs) => list_walk!(gs.content),

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
fn walk_expr_children(e: &Expr, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    let mut aborted = false;
    // Delegate the *child enumeration* to the canonical Expr-level walker, and
    // for each `&Expr` child re-wrap it as a `Node::Expr` clone before invoking
    // the `Node` walker. The Expr-level walker has already-visited semantics
    // identical to C (it recurses into children only), so the set/order of
    // children observed here is exactly the C `expression_tree_walker` child set
    // for the corresponding node tag.
    let mut child_walker = |child: &Expr| -> bool {
        if walker(&Node::Expr(child.clone())) {
            aborted = true;
            return true;
        }
        false
    };
    crate::nodefuncs::expression_tree_walker(Some(e), &mut child_walker);
    aborted
}

/// C `T_TableFunc` arm of `expression_tree_walker` (nodeFuncs.c:2647): WALK
/// each of ns_uris, docexpr, rowexpr, colexprs, coldefexprs, colvalexprs,
/// passingvalexprs in that order. The owned `TableFunc` carries these as
/// `Expr`/`PgVec<PgBox<Expr>>`/`PgVec<Option<PgBox<Expr>>>`; each `&Expr` is
/// re-wrapped as a `Node::Expr` clone for the `FnMut(&Node)` walker (same
/// clone-wrap as [`walk_expr_children`]).
fn walk_table_func(tf: &types_nodes::primnodes::TableFunc, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    // `List *` of `Expr *` (no NULL elements).
    fn list(
        l: &Option<mcx::PgVec<'_, mcx::PgBox<'_, Expr>>>,
        walker: &mut dyn FnMut(&Node) -> bool,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter() {
                if walker(&Node::Expr((**e).clone())) {
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
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter().flatten() {
                if walker(&Node::Expr((**e).clone())) {
                    return true;
                }
            }
        }
        false
    }
    fn opt(e: &Option<mcx::PgBox<'_, Expr>>, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
        match e.as_deref() {
            Some(e) => walker(&Node::Expr(e.clone())),
            None => false,
        }
    }
    list(&tf.ns_uris, walker)
        || opt(&tf.docexpr, walker)
        || opt(&tf.rowexpr, walker)
        || list_opt(&tf.colexprs, walker)
        || list_opt(&tf.coldefexprs, walker)
        || list_opt(&tf.colvalexprs, walker)
        || list(&tf.passingvalexprs, walker)
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

// ===========================================================================
// expression_tree_walker_mut (Node-level, in-place) — for parse_collate
// ===========================================================================

/// `expression_tree_walker` over `&mut Node` (nodeFuncs.c shape, in-place). Used
/// by `parse_collate`'s `assign_collations_walker`, which walks each child,
/// mutates it in place, reads the assigned collation state back, and aborts on a
/// `walker` `true`. Same child set/order as [`expression_tree_walker`]; threads
/// `&mut Node` instead of `&Node`.
pub fn expression_tree_walker_mut(
    node: &mut Node,
    walker: &mut dyn FnMut(&mut Node) -> bool,
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

    match node {
        Node::Expr(e) => walk_expr_children_mut(e, walker),

        Node::RangeTblRef(_)
        | Node::SortGroupClause(_)
        | Node::RowMarkClause(_)
        | Node::A_Star(_)
        | Node::ParamRef(_)
        | Node::Integer(_)
        | Node::Float(_)
        | Node::Boolean(_)
        | Node::String(_)
        | Node::BitString(_) => false,

        Node::Query(_) => false,

        Node::TargetEntry(te) => match te.expr.as_deref_mut() {
            Some(e) => {
                let mut wrapped = Node::Expr(e.clone());
                let aborted = walker(&mut wrapped);
                if let Node::Expr(ne) = wrapped {
                    *e = ne;
                }
                aborted
            }
            None => false,
        },

        Node::FromExpr(from) => list_walk!(from.fromlist) || walk_opt!(from.quals.as_mut()),

        Node::JoinExpr(join) => {
            walk_opt!(join.larg.as_mut())
                || walk_opt!(join.rarg.as_mut())
                || walk_opt!(join.quals.as_mut())
        }

        Node::OnConflictExpr(oce) => {
            list_walk!(oce.arbiterElems)
                || walk_opt!(oce.arbiterWhere.as_mut())
                || list_walk!(oce.onConflictSet)
                || walk_opt!(oce.onConflictWhere.as_mut())
                || list_walk!(oce.exclRelTlist)
        }

        Node::MergeAction(action) => {
            walk_opt!(action.qual.as_mut()) || list_walk!(action.targetList)
        }

        Node::WindowClause(wc) => {
            list_walk!(wc.partitionClause)
                || list_walk!(wc.orderClause)
                || walk_opt!(wc.startOffset.as_mut())
                || walk_opt!(wc.endOffset.as_mut())
        }

        Node::CommonTableExpr(cte) => {
            walk_opt!(cte.ctequery.as_mut()) || walk_opt!(cte.cycle_clause.as_mut())
        }

        Node::CTECycleClause(cc) => {
            walk_opt!(cc.cycle_mark_value.as_mut()) || walk_opt!(cc.cycle_mark_default.as_mut())
        }

        Node::TableFunc(tf) => walk_table_func_mut(tf, walker),

        Node::SetOperationStmt(setop) => {
            walk_opt!(setop.larg.as_mut()) || walk_opt!(setop.rarg.as_mut())
        }

        Node::WithCheckOption(wco) => walk_opt!(wco.qual.as_mut()),

        Node::RangeTblFunction(rtf) => walk_opt!(rtf.funcexpr.as_mut()),

        Node::GroupingSet(gs) => list_walk!(gs.content),

        _ => unrecognized_expression_node(node),
    }
}

/// In-place analogue of [`walk_table_func`]: walk each `Expr` child as a wrapped
/// `Node::Expr`, writing the (possibly mutated) child back into its field.
fn walk_table_func_mut(
    tf: &mut types_nodes::primnodes::TableFunc,
    walker: &mut dyn FnMut(&mut Node) -> bool,
) -> bool {
    fn one(e: &mut Expr, walker: &mut dyn FnMut(&mut Node) -> bool) -> bool {
        let mut wrapped = Node::Expr(e.clone());
        let aborted = walker(&mut wrapped);
        if let Node::Expr(ne) = wrapped {
            *e = ne;
        }
        aborted
    }
    fn list(
        l: &mut Option<mcx::PgVec<'_, mcx::PgBox<'_, Expr>>>,
        walker: &mut dyn FnMut(&mut Node) -> bool,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter_mut() {
                if one(e, walker) {
                    return true;
                }
            }
        }
        false
    }
    fn list_opt(
        l: &mut Option<mcx::PgVec<'_, Option<mcx::PgBox<'_, Expr>>>>,
        walker: &mut dyn FnMut(&mut Node) -> bool,
    ) -> bool {
        if let Some(v) = l {
            for e in v.iter_mut().flatten() {
                if one(e, walker) {
                    return true;
                }
            }
        }
        false
    }
    fn opt(e: &mut Option<mcx::PgBox<'_, Expr>>, walker: &mut dyn FnMut(&mut Node) -> bool) -> bool {
        match e.as_deref_mut() {
            Some(e) => one(e, walker),
            None => false,
        }
    }
    list(&mut tf.ns_uris, walker)
        || opt(&mut tf.docexpr, walker)
        || opt(&mut tf.rowexpr, walker)
        || list_opt(&mut tf.colexprs, walker)
        || list_opt(&mut tf.coldefexprs, walker)
        || list_opt(&mut tf.colvalexprs, walker)
        || list(&mut tf.passingvalexprs, walker)
}

/// In-place analogue of [`walk_expr_children`]: walk each `Expr` child as a
/// wrapped `Node::Expr`, then write the (possibly mutated) child back into its
/// field. Uses the canonical Expr-level in-place driver
/// [`crate::nodefuncs`]'s child set via a per-child wrap/unwrap.
fn walk_expr_children_mut(e: &mut Expr, walker: &mut dyn FnMut(&mut Node) -> bool) -> bool {
    let mut aborted = false;
    crate::nodefuncs::for_each_expr_child_mut(e, &mut |child: &mut Expr| {
        if aborted {
            return;
        }
        let mut wrapped = Node::Expr(child.clone());
        if walker(&mut wrapped) {
            aborted = true;
        }
        if let Node::Expr(nc) = wrapped {
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

    match node {
        // leaves the raw walker stops at (C: T_SetToDefault/T_CurrentOfExpr/
        // T_Integer/.../T_ParamRef/T_A_Const/T_A_Star → break)
        Node::Integer(_)
        | Node::Float(_)
        | Node::Boolean(_)
        | Node::String(_)
        | Node::BitString(_)
        | Node::ParamRef(_)
        | Node::A_Star(_)
        | Node::RangeVar(_) => false,

        Node::A_Const(_) => false,

        Node::ColumnRef(cref) => list_walk!(cref.fields),

        Node::A_Expr(a) => {
            walk_opt!(a.lexpr.as_ref()) || walk_opt!(a.rexpr.as_ref())
        }

        Node::FuncCall(fc) => {
            list_walk!(fc.args)
                || list_walk!(fc.agg_order)
                || walk_opt!(fc.agg_filter.as_ref())
                || match fc.over.as_deref() {
                    Some(w) => raw_walk_windowdef(w, walker),
                    None => false,
                }
        }

        Node::A_Indices(ai) => {
            walk_opt!(ai.lidx.as_ref()) || walk_opt!(ai.uidx.as_ref())
        }

        Node::A_Indirection(ai) => {
            walk_opt!(ai.arg.as_ref()) || list_walk!(ai.indirection)
        }

        Node::A_ArrayExpr(aae) => list_walk!(aae.elements),

        Node::ResTarget(rt) => list_walk!(rt.indirection) || walk_opt!(rt.val.as_ref()),

        Node::MultiAssignRef(mar) => walk_opt!(mar.source.as_ref()),

        Node::TypeCast(tc) => walk_opt!(tc.arg.as_ref()),

        Node::CollateClause(cc) => walk_opt!(cc.arg.as_ref()),

        Node::SortBy(sb) => walk_opt!(sb.node.as_ref()),

        Node::WindowDef(wd) => raw_walk_windowdef(wd, walker),

        Node::RangeSubselect(rs) => walk_opt!(rs.subquery.as_ref()),

        Node::RangeFunction(rf) => list_walk!(rf.functions),

        Node::RangeTableSample(rts) => {
            walk_opt!(rts.relation.as_ref())
                || list_walk!(rts.args)
                || walk_opt!(rts.repeatable.as_ref())
        }

        Node::TypeName(tn) => {
            list_walk!(tn.typmods) || list_walk!(tn.arrayBounds)
        }

        Node::ColumnDef(cd) => {
            (match cd.typeName.as_deref() {
                Some(tn) => raw_walk_typename(tn, walker),
                None => false,
            }) || walk_opt!(cd.raw_default.as_ref())
            // C also WALKs collClause/constraints, but those are typed
            // sub-objects (CollateClause/Constraint) — collClause is recursed via
            // its `Node` arm below; Constraint is not yet in the Node universe.
        }

        Node::GroupingSet(gs) => list_walk!(gs.content),

        Node::SelectStmt(s) => raw_walk_selectstmt(s, walker),

        Node::InsertStmt(i) => {
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

        Node::UpdateStmt(u) => {
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

        Node::DeleteStmt(d) => {
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

        Node::MergeStmt(m) => {
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

        Node::MergeWhenClause(mwc) => {
            walk_opt!(mwc.condition.as_ref())
                || list_walk!(mwc.targetList)
                || list_walk!(mwc.values)
        }

        Node::CommonTableExpr(cte) => walk_opt!(cte.ctequery.as_ref()),

        Node::JoinExpr(join) => {
            walk_opt!(join.larg.as_ref())
                || walk_opt!(join.rarg.as_ref())
                || walk_opt!(join.quals.as_ref())
        }

        Node::FromExpr(from) => list_walk!(from.fromlist) || walk_opt!(from.quals.as_ref()),

        // a sub-Query (post-analysis) embedded in raw output is walked by
        // recursing the central expression walker over it
        Node::Query(_) => false,

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

/// `query_tree_walker(query, walker, context, flags)` (nodeFuncs.c) — apply
/// `walker` to all the expression trees hanging off a `Query`, then recurse into
/// sub-queries via the range table per `flags`. Returns `true` on abort.
pub fn query_tree_walker(
    query: &types_nodes::copy_query::Query,
    walker: &mut dyn FnMut(&Node) -> bool,
    flags: i32,
) -> bool {
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

    // targetList / returningList are `Vec<TargetEntry>` (typed), wrapped per-elem.
    if walk_targetentry_list(&query.targetList, walker) {
        return true;
    }
    if let Some(oc) = query.onConflict.as_deref() {
        if walk_onconflict_expr(oc, walker) {
            return true;
        }
    }
    if walk_targetentry_list(&query.returningList, walker) {
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
    if walk_opt!(query.mergeJoinCondition.as_ref()) {
        return true;
    }
    if list_walk!(query.windowClause) {
        return true;
    }
    if walk_opt!(query.havingQual.as_ref()) {
        return true;
    }
    if walk_opt!(query.limitOffset.as_ref()) {
        return true;
    }
    if walk_opt!(query.limitCount.as_ref()) {
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
    // plain `false`/visit-only otherwise. cteList descends via range_table only.
    if list_walk!(query.cteList) {
        return true;
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
                    if query_tree_walker(sub, walker, flags) {
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
            if list_walk!(rte.groupexprs) {
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
    match node {
        Node::Query(q) => query_tree_walker(q, walker, flags),
        other => walker(other),
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
pub fn query_tree_mutator(
    query: &mut types_nodes::copy_query::Query,
    mutator: &mut dyn FnMut(&mut Node) -> bool,
    flags: i32,
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

    if mutate_targetentry_list(&mut query.targetList, mutator) {
        return true;
    }
    if mutate_targetentry_list(&mut query.returningList, mutator) {
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
    if walk_opt!(query.mergeJoinCondition.as_mut()) {
        return true;
    }
    if list_walk!(query.windowClause) {
        return true;
    }
    if walk_opt!(query.havingQual.as_mut()) {
        return true;
    }
    if walk_opt!(query.limitOffset.as_mut()) {
        return true;
    }
    if walk_opt!(query.limitCount.as_mut()) {
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
        if range_table_mutator(&mut query.rtable, mutator, flags) {
            return true;
        }
    }
    false
}

/// `range_table_mutator(rtable, mutator, context, flags)` (nodeFuncs.c) —
/// in-place owned-tree variant.
pub fn range_table_mutator(
    rtable: &mut [types_nodes::parsenodes::RangeTblEntry],
    mutator: &mut dyn FnMut(&mut Node) -> bool,
    flags: i32,
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
                        if query_tree_mutator(sub, mutator, flags) {
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
                if list_walk!(rte.groupexprs) {
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
pub fn query_or_expression_tree_mutator(
    node: &mut Node,
    mutator: &mut dyn FnMut(&mut Node) -> bool,
    flags: i32,
) -> bool {
    match node {
        Node::Query(q) => query_tree_mutator(q, mutator, flags),
        other => mutator(other),
    }
}

fn mutate_targetentry_list(
    list: &mut [types_nodes::primnodes::TargetEntry],
    mutator: &mut dyn FnMut(&mut Node) -> bool,
) -> bool {
    for te in list.iter_mut() {
        if let Some(e) = te.expr.as_deref_mut() {
            let mut wrapped = Node::Expr(e.clone());
            let aborted = mutator(&mut wrapped);
            if let Node::Expr(ne) = wrapped {
                *e = ne;
            }
            if aborted {
                return true;
            }
        }
    }
    false
}

fn mutate_fromexpr(
    from: &mut types_nodes::rawnodes::FromExpr,
    mutator: &mut dyn FnMut(&mut Node) -> bool,
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
        assert_eq!(Node::Integer(Integer { ival: 7 }).tag(), T_Integer);
        assert_eq!(Node::Boolean(Boolean { boolval: true }).tag(), T_Boolean);
        assert_eq!(
            Node::Float(Float {
                fval: mcx::PgString::from_str_in("1.5", mcx).unwrap()
            })
            .tag(),
            T_Float
        );
        assert_eq!(
            Node::String(StringNode {
                sval: mcx::PgString::from_str_in("x", mcx).unwrap()
            })
            .tag(),
            T_String
        );
        assert_eq!(
            Node::BitString(BitString {
                bsval: mcx::PgString::from_str_in("b101", mcx).unwrap()
            })
            .tag(),
            T_BitString
        );
        // value nodes are leaves: no children walked
        let node = Node::Integer(Integer { ival: 7 });
        assert!(!expression_tree_walker(&node, &mut |_c| panic!("leaf has no children")));
    }

    #[test]
    fn node_walker_descends_nodeptr_children() {
        // FromExpr { fromlist: [JoinExpr{quals: OpExpr(Var,Var)}], quals: Var }
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();

        let join = Node::JoinExpr(JoinExpr {
            jointype: JoinType::JOIN_INNER,
            isNatural: false,
            larg: None,
            rarg: None,
            usingClause: mcx::PgVec::new_in(mcx),
            join_using_alias: None,
            quals: Some(mcx::alloc_in(mcx, Node::Expr(op_with_two_vars())).unwrap()),
            alias: None,
            rtindex: 0,
        });

        let mut fromlist = mcx::PgVec::new_in(mcx);
        fromlist.push(mcx::alloc_in(mcx, join).unwrap());
        let from = FromExpr {
            fromlist,
            quals: Some(mcx::alloc_in(mcx, Node::Expr(Expr::Var(Var::default()))).unwrap()),
        };
        let node = Node::FromExpr(from);

        // Top-level children: the JoinExpr and the quals Var.
        let mut tags = Vec::new();
        expression_tree_walker(&node, &mut |c: &Node| {
            tags.push(c.tag());
            false
        });
        assert_eq!(tags.len(), 2);
        assert!(node.as_var().is_none());
    }

    #[test]
    fn mut_walker_writes_child_back() {
        // Mutate each Var child's varno via the in-place Node walker.
        let mut node = Node::Expr(op_with_two_vars());
        let aborted = expression_tree_walker_mut(&mut node, &mut |c: &mut Node| {
            if let Some(v) = c.as_var_mut() {
                v.varno = 42;
            }
            false
        });
        assert!(!aborted);
        if let Node::Expr(Expr::OpExpr(o)) = &node {
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
) -> bool {
    // C visits `(Node *) tle`, whose `T_TargetEntry` arm WALKs `tle->expr`. In
    // the split `PgBox`-based model a `TargetEntry` cannot be re-wrapped into a
    // `Node` without an allocator (its `expr` is a `PgBox`), so we WALK the
    // expression directly as `Node::Expr` — the same child the TargetEntry arm
    // would reach. The lifetime-free `Expr` clone is total.
    for te in list {
        if let Some(e) = te.expr.as_deref() {
            if walker(&Node::Expr(e.clone())) {
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

