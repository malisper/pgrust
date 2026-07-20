//! nodeFuncs.c walker/mutator halves over the opaque `Node`. C walkers see
//! inline-`List` fields and `Query`/`FromExpr`/`SelectStmt`/`Alias`
//! sub-structs as bare `Node *`; this vocabulary stores lists by value and
//! those structs by reference, so list fields are walked element-wise (no
//! walker call on the `List` itself) and struct-valued refs dispatch through
//! the [`NodeWalker`] `visit_*_ref` hooks — identical semantics unless a
//! walker special-cases those tags, in which case it overrides the hooks.
//! The mutator is identity-preserving: `None` = unchanged, share the input
//! (sound: sealed nodes are immutable, one arena lifetime). Walks allocate
//! nothing (fabled #417); the mutator allocates only after the first change.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Aggref, Alias, FromExpr, FuncExpr, MinMaxExpr, OpExpr, TargetEntry};
use types_nodes::rawnodes::SelectStmt;
use types_nodes::{Node, NodeList, NodeTag};

#[cfg(test)]
mod tests;

pub mod makefuncs;
pub mod node_funcs;
pub mod print;
pub use node_funcs::{
    expr_collation, expr_input_collation, expr_is_null_constant, expr_location, expr_type,
    expr_typmod, relabel_to_typmod, set_opfuncid, set_sa_opfuncid,
};

pub const QTW_IGNORE_RT_SUBQUERIES: u32 = 0x01;
pub const QTW_IGNORE_CTE_SUBQUERIES: u32 = 0x02;
pub const QTW_IGNORE_RC_SUBQUERIES: u32 = 0x03;
pub const QTW_IGNORE_JOINALIASES: u32 = 0x04;
pub const QTW_IGNORE_RANGE_TABLE: u32 = 0x08;
pub const QTW_EXAMINE_RTES_BEFORE: u32 = 0x10;
pub const QTW_EXAMINE_RTES_AFTER: u32 = 0x20;
pub const QTW_DONT_COPY_QUERY: u32 = 0x40;
pub const QTW_EXAMINE_SORTGROUP: u32 = 0x80;
pub const QTW_IGNORE_GROUPEXPRS: u32 = 0x100;

#[cold]
#[inline(never)]
pub fn deferred(what: &str, tag: NodeTag) -> ! {
    panic!("nodeFuncs deferred arm: {what} ({tag:?}) — node vocabulary unported");
}

pub trait NodeWalker<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool>;

    /// Receives `RangeTblEntry.subquery` (stored as `&Query`, not a `Node`).
    /// Default mirrors expression_tree_walker's `T_Query` no-op; walkers that
    /// descend into subqueries override this together with their `T_Query` arm.
    fn visit_query_ref(&mut self, _q: &'mcx Query<'mcx>) -> PgResult<bool> {
        Ok(false)
    }

    /// `SelectStmt.larg`/`rarg`. Default descends into the sub-select's
    /// fields (C's net effect for a callback that recurses on unknown tags)
    /// but skips the callback on the SelectStmt itself.
    fn visit_select_stmt_ref(&mut self, s: &'mcx SelectStmt<'mcx>) -> PgResult<bool> {
        walk_select_stmt(s, self)
    }

    /// `RangeVar.alias`. Default mirrors the raw walker's `T_Alias` no-op,
    /// skipping the callback on the Alias itself.
    fn visit_alias_ref(&mut self, _a: &'mcx Alias<'mcx>) -> PgResult<bool> {
        Ok(false)
    }
}

pub fn walk_list<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    list: &NodeList<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    for n in list {
        if w.visit(n)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn walk_opt<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Option<Node<'mcx>>,
    w: &mut W,
) -> PgResult<bool> {
    match node {
        Some(n) => w.visit(n),
        None => Ok(false),
    }
}

fn walk_from_expr<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    f: &'mcx FromExpr<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    Ok(walk_list(&f.fromlist, w)? || walk_opt(f.quals, w)?)
}

fn walk_opt_list<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    list: &types_nodes::OptNodeList<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    for n in list.iter().flatten() {
        if w.visit(n)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Thin generic wrapper — coerces the caller's concrete `W` to a single
/// `&mut dyn NodeWalker` and delegates to the one monomorphic engine below.
/// De-monomorphization "byte-shell" pattern (cf. the mutator half, af0dfc02d):
/// N callsites each stamp out this ~3-line shell instead of a full ~1,600-line
/// copy of the `NodeTag` match. Behavior is identical to a directly-generic
/// body. `W: Sized` (was `?Sized`): every caller in-tree passes a concrete
/// walker — no `dyn NodeWalker` exists anywhere — so the coercion is always
/// available; the sole `?Sized` consumer (the trait default `visit_select_stmt_ref`)
/// routes through `walk_select_stmt`, which stays generic.
pub fn expression_tree_walker<'mcx, W: NodeWalker<'mcx>>(
    node: Node<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    expression_tree_walker_dyn(node, w)
}

/// Monomorphic engine: the walker is type-erased to `&mut dyn NodeWalker`, so
/// this large `NodeTag` match is codegen'd exactly once regardless of how many
/// distinct walker types call it. Cold (planner/rewriter/parser) path — the
/// extra indirect call per child is planning-time only, not per-row.
pub fn expression_tree_walker_dyn<'mcx>(
    node: Node<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
) -> PgResult<bool> {
    match node.node_tag() {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_SortGroupClause
        | NodeTag::T_CTESearchClause
        | NodeTag::T_MergeSupportFunc => Ok(false),
        NodeTag::T_WithCheckOption => {
            walk_opt(node.as_with_check_option().unwrap().qual, w)
        }
        NodeTag::T_Aggref => {
            let a = node.as_variant::<Aggref>().unwrap();
            Ok(walk_list(&a.aggdirectargs, w)?
                || walk_list(&a.args, w)?
                || walk_list(&a.aggorder, w)?
                || walk_list(&a.aggdistinct, w)?
                || walk_opt(a.aggfilter, w)?)
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            Ok(walk_list(&wf.args, w)?
                || walk_opt(wf.aggfilter, w)?
                || walk_list(&wf.runCondition, w)?)
        }
        NodeTag::T_WindowFuncRunCondition => {
            w.visit(node.as_window_func_run_condition().unwrap().arg)
        }
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            walk_list(&g.args, w)
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            walk_list(&f.args, w)
        }
        NodeTag::T_NamedArgExpr => w.visit(
            node.as_named_arg_expr()
                .unwrap()
                .arg
                .expect("NamedArgExpr has an arg"),
        ),
        NodeTag::T_OpExpr => {
            let o = node.as_variant::<OpExpr>().unwrap();
            walk_list(&o.args, w)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            walk_list(&sa.args, w)
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            walk_list(&a.elements, w)
        }
        NodeTag::T_SubscriptingRef => {
            let s = node.as_subscripting_ref().unwrap();
            Ok(walk_opt_list(&s.refupperindexpr, w)?
                || walk_opt_list(&s.reflowerindexpr, w)?
                || walk_opt(s.refexpr, w)?
                || walk_opt(s.refassgnexpr, w)?)
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            walk_list(&b.args, w)
        }
        NodeTag::T_NullTest => walk_opt(node.as_null_test().unwrap().arg, w),
        NodeTag::T_RelabelType => w.visit(node.as_relabel_type().unwrap().arg),
        NodeTag::T_FieldSelect => w.visit(node.as_field_select().unwrap().arg),
        NodeTag::T_ReturningExpr => w.visit(node.as_returning_expr().unwrap().retexpr),
        NodeTag::T_CollateExpr => w.visit(node.as_collate_expr().unwrap().arg),
        NodeTag::T_CoerceViaIO => w.visit(node.as_coerce_via_io().unwrap().arg),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            Ok(w.visit(a.arg)? || walk_opt(a.elemexpr, w)?)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            w.visit(node.as_convert_rowtype_expr().unwrap().arg)
        }
        NodeTag::T_BooleanTest => walk_opt(node.as_boolean_test().unwrap().arg, w),
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            walk_list(&d.args, w)
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            walk_list(&d.args, w)
        }
        NodeTag::T_RowExpr => {
            // C notes: don't examine row_typeid/colnames.
            let r = node.as_row_expr().unwrap();
            walk_list(&r.args, w)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            Ok(walk_opt(j.raw_expr, w)? || walk_opt(j.formatted_expr, w)?)
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            Ok(walk_list(&c.args, w)? || walk_opt(c.func, w)? || walk_opt(c.coercion, w)?)
        }
        NodeTag::T_JsonIsPredicate => walk_opt(node.as_json_is_predicate().unwrap().expr, w),
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            // C: "we assume walker doesn't care about passing_names".
            Ok(walk_opt(j.formatted_expr, w)?
                || walk_opt(j.path_spec, w)?
                || walk_list(&j.passing_values, w)?
                || walk_opt(j.on_empty, w)?
                || walk_opt(j.on_error, w)?)
        }
        NodeTag::T_JsonBehavior => walk_opt(node.as_json_behavior().unwrap().expr, w),
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            Ok(walk_list(&rc.largs, w)? || walk_list(&rc.rargs, w)?)
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            Ok(w.visit(fs.arg)? || walk_list(&fs.newvals, w)?)
        }
        NodeTag::T_CoerceToDomain => w.visit(node.as_coerce_to_domain().unwrap().arg),
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            walk_list(&mm.args, w)
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            walk_list(&co.args, w)
        }
        // C walks straight through CaseWhen cells (walker "doesn't care").
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if walk_opt(c.arg, w)? {
                return Ok(true);
            }
            for cell in &c.args {
                let cw = cell.as_case_when().expect("CaseWhen");
                if walk_opt(cw.expr, w)? || walk_opt(cw.result, w)? {
                    return Ok(true);
                }
            }
            walk_opt(c.defresult, w)
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            Ok(walk_opt(cw.expr, w)? || walk_opt(cw.result, w)?)
        }
        NodeTag::T_TargetEntry => {
            let te = node.as_variant::<TargetEntry>().unwrap();
            w.visit(te.expr)
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            // C walks the subselect Query node too, so walkers can recurse.
            Ok(walk_opt(sl.testexpr, w)? || w.visit(sl.subselect)?)
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            Ok(walk_opt(sp.testexpr, w)? || walk_list(&sp.args, w)?)
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            w.visit(phv.phexpr)
        }
        NodeTag::T_AlternativeSubPlan => {
            let asp = node.as_alternative_sub_plan().unwrap();
            walk_list(&asp.subplans, w)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            walk_from_expr(f, w)
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            Ok(w.visit(j.larg)? || w.visit(j.rarg)? || walk_opt(j.quals, w)?)
        }
        NodeTag::T_Query => Ok(false),
        NodeTag::T_SetOperationStmt => {
            // C walks only larg/rarg (groupClauses deemed uninteresting).
            let s = node.as_set_operation_stmt().unwrap();
            Ok(walk_opt(s.larg, w)? || walk_opt(s.rarg, w)?)
        }
        NodeTag::T_CommonTableExpr => {
            // C walks only ctequery (search/cycle clauses uninteresting here).
            let cte = node.as_common_table_expr().unwrap();
            walk_opt(cte.ctequery, w)
        }
        NodeTag::T_MergeAction => {
            let a = node.as_merge_action().unwrap();
            Ok(walk_opt(a.qual, w)? || walk_list(&a.targetList, w)?)
        }
        NodeTag::T_List => walk_list(node.as_list().unwrap(), w),
        NodeTag::T_RangeTblFunction => {
            walk_opt(node.as_range_tbl_function().unwrap().funcexpr, w)
        }
        NodeTag::T_TableSampleClause => {
            let tsc = node.as_table_sample_clause().unwrap();
            Ok(walk_list(&tsc.args, w)? || walk_opt(tsc.repeatable, w)?)
        }
        // C: arg_names and ns_names deemed uninteresting.
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            Ok(walk_list(&x.named_args, w)? || walk_list(&x.args, w)?)
        }
        NodeTag::T_TableFunc => {
            let tf = node.as_table_func().unwrap();
            Ok(walk_list(&tf.ns_uris, w)?
                || walk_opt(tf.docexpr, w)?
                || walk_opt(tf.rowexpr, w)?
                || walk_opt_list(&tf.colexprs, w)?
                || walk_opt_list(&tf.coldefexprs, w)?
                || walk_opt_list(&tf.colvalexprs, w)?
                || walk_list(&tf.passingvalexprs, w)?)
        }
        NodeTag::T_InferenceElem => walk_opt(node.as_inference_elem().unwrap().expr, w),
        NodeTag::T_OnConflictExpr => {
            let oc = node.as_on_conflict_expr().unwrap();
            Ok(walk_list(&oc.arbiterElems, w)?
                || walk_opt(oc.arbiterWhere, w)?
                || walk_list(&oc.onConflictSet, w)?
                || walk_opt(oc.onConflictWhere, w)?
                || walk_list(&oc.exclRelTlist, w)?)
        }
        NodeTag::T_PartitionBoundSpec => {
            let pbs = node.as_variant::<types_nodes::rawnodes::PartitionBoundSpec>().unwrap();
            Ok(walk_list(&pbs.listdatums, w)?
                || walk_list(&pbs.lowerdatums, w)?
                || walk_list(&pbs.upperdatums, w)?)
        }
        // Range-bound list elements: MINVALUE/MAXVALUE carry no value node.
        NodeTag::T_PartitionRangeDatum => {
            let prd = node.as_variant::<types_nodes::rawnodes::PartitionRangeDatum>().unwrap();
            walk_opt(prd.value, w)
        }
        other => deferred("expression_tree_walker", other),
    }
}

/// Generic wrapper over the erased-walker engine (de-mono byte-shell).
pub fn query_tree_walker<'mcx, W: NodeWalker<'mcx>>(
    query: &Query<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    query_tree_walker_dyn(query, w, flags)
}

/// Monomorphic engine (single copy); see `expression_tree_walker_dyn`.
pub fn query_tree_walker_dyn<'mcx>(
    query: &Query<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
    flags: u32,
) -> PgResult<bool> {
    if walk_list(&query.targetList, w)?
        || walk_list(&query.withCheckOptions, w)?
        || walk_opt(query.onConflict, w)?
        || walk_list(&query.mergeActionList, w)?
        || walk_opt(query.mergeJoinCondition, w)?
        || walk_list(&query.returningList, w)?
    {
        return Ok(true);
    }
    if let Some(jt) = query.jointree {
        if walk_from_expr(jt, w)? {
            return Ok(true);
        }
    }
    if walk_opt(query.setOperations, w)?
        || walk_opt(query.havingQual, w)?
        || walk_opt(query.limitOffset, w)?
        || walk_opt(query.limitCount, w)?
    {
        return Ok(true);
    }
    if flags & QTW_EXAMINE_SORTGROUP != 0 {
        if walk_list(&query.groupClause, w)?
            || walk_list(&query.windowClause, w)?
            || walk_list(&query.sortClause, w)?
            || walk_list(&query.distinctClause, w)?
        {
            return Ok(true);
        }
    } else {
        for wc_node in &query.windowClause {
            let wc = wc_node.as_window_clause().expect("windowClause element");
            if walk_opt(wc.startOffset, w)? || walk_opt(wc.endOffset, w)? {
                return Ok(true);
            }
        }
    }
    if flags & QTW_IGNORE_CTE_SUBQUERIES == 0 && walk_list(&query.cteList, w)? {
        return Ok(true);
    }
    if flags & QTW_IGNORE_RANGE_TABLE == 0 && range_table_walker_dyn(&query.rtable, w, flags)? {
        return Ok(true);
    }
    Ok(false)
}

/// Generic wrapper over the erased-walker engine (de-mono byte-shell).
pub fn range_table_walker<'mcx, W: NodeWalker<'mcx>>(
    rtable: &NodeList<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    range_table_walker_dyn(rtable, w, flags)
}

/// Monomorphic engine (single copy); see `expression_tree_walker_dyn`.
pub fn range_table_walker_dyn<'mcx>(
    rtable: &NodeList<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
    flags: u32,
) -> PgResult<bool> {
    for rte in rtable {
        if range_table_entry_walker_dyn(rte, w, flags)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Generic wrapper over the erased-walker engine (de-mono byte-shell).
pub fn range_table_entry_walker<'mcx, W: NodeWalker<'mcx>>(
    rte_node: Node<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    range_table_entry_walker_dyn(rte_node, w, flags)
}

/// Monomorphic engine (single copy); see `expression_tree_walker_dyn`.
pub fn range_table_entry_walker_dyn<'mcx>(
    rte_node: Node<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
    flags: u32,
) -> PgResult<bool> {
    let rte: &RangeTblEntry<'mcx> = rte_node
        .as_range_tbl_entry()
        .unwrap_or_else(|| panic!("rtable element is not a RangeTblEntry: {:?}", rte_node));
    if flags & QTW_EXAMINE_RTES_BEFORE != 0 && w.visit(rte_node)? {
        return Ok(true);
    }
    let hit = match rte.rtekind {
        RTEKind::RTE_RELATION => walk_opt(rte.tablesample, w)?,
        RTEKind::RTE_SUBQUERY => {
            if flags & QTW_IGNORE_RT_SUBQUERIES == 0 {
                match rte.subquery {
                    Some(q) => w.visit_query_ref(q)?,
                    None => false,
                }
            } else {
                false
            }
        }
        RTEKind::RTE_JOIN => {
            flags & QTW_IGNORE_JOINALIASES == 0 && walk_list(&rte.joinaliasvars, w)?
        }
        RTEKind::RTE_FUNCTION => walk_list(&rte.functions, w)?,
        RTEKind::RTE_TABLEFUNC => walk_opt(rte.tablefunc, w)?,
        RTEKind::RTE_VALUES => walk_list(&rte.values_lists, w)?,
        RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE | RTEKind::RTE_RESULT => false,
        RTEKind::RTE_GROUP => {
            flags & QTW_IGNORE_GROUPEXPRS == 0 && walk_list(&rte.groupexprs, w)?
        }
    };
    if hit {
        return Ok(true);
    }
    if walk_list(&rte.securityQuals, w)? {
        return Ok(true);
    }
    if flags & QTW_EXAMINE_RTES_AFTER != 0 && w.visit(rte_node)? {
        return Ok(true);
    }
    Ok(false)
}

/// Generic wrapper over the erased-walker engine (de-mono byte-shell).
pub fn query_or_expression_tree_walker<'mcx, W: NodeWalker<'mcx>>(
    node: Node<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    query_or_expression_tree_walker_dyn(node, w, flags)
}

/// Monomorphic engine (single copy); see `expression_tree_walker_dyn`.
pub fn query_or_expression_tree_walker_dyn<'mcx>(
    node: Node<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
    flags: u32,
) -> PgResult<bool> {
    match node.as_query() {
        Some(q) => query_tree_walker_dyn(q, w, flags),
        None => w.visit(node),
    }
}

/// C query_or_expression_tree_mutator; the Query arm needs the generic
/// query_tree_mutator engine (unported — rewrite_manip carries the only
/// specialized form), so it panics loudly.
pub fn query_or_expression_tree_mutator<'mcx, F>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    m: &mut F,
    _flags: u32,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    let _ = mcx;
    if node.as_query().is_some() {
        panic!(
            "query_or_expression_tree_mutator (nodeFuncs.c): generic              query_tree_mutator engine unported — nodes-core lane"
        );
    }
    m(node)
}

pub fn walk_select_stmt<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    s: &'mcx SelectStmt<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    if let types_nodes::DistinctClause::On(l) = &s.distinctClause {
        if walk_list(l, w)? {
            return Ok(true);
        }
    }
    if walk_opt(s.intoClause, w)?
        || walk_list(&s.targetList, w)?
        || walk_list(&s.fromClause, w)?
        || walk_opt(s.whereClause, w)?
        || walk_list(&s.groupClause, w)?
        || walk_opt(s.havingClause, w)?
        || walk_list(&s.windowClause, w)?
        || walk_list(&s.valuesLists, w)?
        || walk_list(&s.sortClause, w)?
        || walk_opt(s.limitOffset, w)?
        || walk_opt(s.limitCount, w)?
        || walk_list(&s.lockingClause, w)?
        || walk_opt(s.withClause, w)?
    {
        return Ok(true);
    }
    if let Some(larg) = s.larg {
        if w.visit_select_stmt_ref(larg)? {
            return Ok(true);
        }
    }
    if let Some(rarg) = s.rarg {
        if w.visit_select_stmt_ref(rarg)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Generic wrapper over the erased-walker engine (de-mono byte-shell).
pub fn raw_expression_tree_walker<'mcx, W: NodeWalker<'mcx>>(
    node: Node<'mcx>,
    w: &mut W,
) -> PgResult<bool> {
    raw_expression_tree_walker_dyn(node, w)
}

/// Monomorphic engine (single copy); see `expression_tree_walker_dyn`. The
/// `T_SelectStmt` arm delegates to `walk_select_stmt`, which stays generic
/// (its `?Sized` is required by the `visit_select_stmt_ref` trait default);
/// here it is instantiated once with `W = dyn NodeWalker`.
pub fn raw_expression_tree_walker_dyn<'mcx>(
    node: Node<'mcx>,
    w: &mut dyn NodeWalker<'mcx>,
) -> PgResult<bool> {
    match node.node_tag() {
        NodeTag::T_JsonFormat
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_Integer
        | NodeTag::T_Float
        | NodeTag::T_Boolean
        | NodeTag::T_String
        | NodeTag::T_BitString
        | NodeTag::T_ParamRef
        | NodeTag::T_A_Const
        | NodeTag::T_A_Star
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_ReturningOption => Ok(false),
        // C: "we assume the colnames list isn't interesting".
        NodeTag::T_Alias => Ok(false),
        NodeTag::T_RangeVar => match node.as_range_var().unwrap().alias {
            Some(a) => w.visit_alias_ref(a),
            None => Ok(false),
        },
        NodeTag::T_A_Expr => {
            let e = node.as_a_expr().unwrap();
            // C: "operator name is deemed uninteresting".
            Ok(walk_opt(e.lexpr, w)? || walk_opt(e.rexpr, w)?)
        }
        // C: "we assume the fields contain nothing interesting".
        NodeTag::T_ColumnRef => Ok(false),
        NodeTag::T_A_Indices => {
            let ai = node.as_a_indices().unwrap();
            Ok(walk_opt(ai.lidx, w)? || walk_opt(ai.uidx, w)?)
        }
        NodeTag::T_A_Indirection => {
            let ind = node.as_a_indirection().unwrap();
            Ok(walk_opt(ind.arg, w)? || walk_list(&ind.indirection, w)?)
        }
        NodeTag::T_A_ArrayExpr => walk_list(&node.as_a_array_expr().unwrap().elements, w),
        NodeTag::T_ResTarget => {
            let rt = node.as_res_target().unwrap();
            Ok(walk_list(&rt.indirection, w)? || walk_opt(rt.val, w)?)
        }
        NodeTag::T_SortBy => walk_opt(node.as_sort_by().unwrap().node, w),
        NodeTag::T_TypeCast => {
            let tc = node.as_type_cast().unwrap();
            Ok(walk_opt(tc.arg, w)? || walk_opt(tc.typeName, w)?)
        }
        NodeTag::T_TypeName => {
            let tn = node.as_type_name().unwrap();
            Ok(walk_list(&tn.typmods, w)? || walk_list(&tn.arrayBounds, w)?)
        }
        NodeTag::T_FuncCall => {
            let fc = node.as_func_call().unwrap();
            Ok(walk_list(&fc.args, w)?
                || walk_list(&fc.agg_order, w)?
                || walk_opt(fc.agg_filter, w)?
                || walk_opt(fc.over, w)?)
        }
        NodeTag::T_SelectStmt => walk_select_stmt(node.as_select_stmt().unwrap(), w),
        NodeTag::T_WindowDef => {
            let wd = node.as_window_def().unwrap();
            Ok(walk_list(&wd.partitionClause, w)?
                || walk_list(&wd.orderClause, w)?
                || walk_opt(wd.startOffset, w)?
                || walk_opt(wd.endOffset, w)?)
        }
        NodeTag::T_BooleanTest => walk_opt(node.as_boolean_test().unwrap().arg, w),
        // C: "we assume the collname is uninteresting".
        NodeTag::T_CollateClause => walk_opt(node.as_collate_clause().unwrap().arg, w),
        NodeTag::T_RowExpr => walk_list(&node.as_row_expr().unwrap().args, w),
        NodeTag::T_MergeAction => {
            let a = node.as_merge_action().unwrap();
            Ok(walk_opt(a.qual, w)? || walk_list(&a.targetList, w)?)
        }
        NodeTag::T_RangeTableFunc => {
            let rtf = node.as_range_table_func().unwrap();
            Ok(walk_opt(rtf.docexpr, w)?
                || walk_opt(rtf.rowexpr, w)?
                || walk_list(&rtf.namespaces, w)?
                || walk_list(&rtf.columns, w)?
                || match rtf.alias {
                    Some(a) => w.visit_alias_ref(a)?,
                    None => false,
                })
        }
        NodeTag::T_RangeTableFuncCol => {
            let rtfc = node.as_range_table_func_col().unwrap();
            Ok(walk_opt(rtfc.colexpr, w)? || walk_opt(rtfc.coldefexpr, w)?)
        }
        NodeTag::T_XmlSerialize => {
            let xs = node.as_xml_serialize().unwrap();
            Ok(walk_opt(xs.expr, w)? || walk_opt(xs.typeName, w)?)
        }
        NodeTag::T_List => walk_list(node.as_list().unwrap(), w),
        // JsonFormat/JsonReturning subtrees are leaves here (typed refs, no
        // expressions inside; C walks them as nodes — divergence, no walker
        // inspects them).
        NodeTag::T_JsonReturning => Ok(false),
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            Ok(walk_opt(j.raw_expr, w)? || walk_opt(j.formatted_expr, w)?)
        }
        NodeTag::T_JsonParseExpr => {
            let j = node.as_json_parse_expr().unwrap();
            Ok(walk_opt(j.expr, w)? || walk_opt(j.output, w)?)
        }
        NodeTag::T_JsonScalarExpr => {
            let j = node.as_json_scalar_expr().unwrap();
            Ok(walk_opt(j.expr, w)? || walk_opt(j.output, w)?)
        }
        NodeTag::T_JsonSerializeExpr => {
            let j = node.as_json_serialize_expr().unwrap();
            Ok(walk_opt(j.expr, w)? || walk_opt(j.output, w)?)
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            Ok(walk_list(&c.args, w)? || walk_opt(c.func, w)? || walk_opt(c.coercion, w)?)
        }
        NodeTag::T_JsonIsPredicate => walk_opt(node.as_json_is_predicate().unwrap().expr, w),
        NodeTag::T_JsonArgument => walk_opt(node.as_json_argument().unwrap().val, w),
        NodeTag::T_JsonBehavior => walk_opt(node.as_json_behavior().unwrap().expr, w),
        NodeTag::T_JsonFuncExpr => {
            let f = node.as_json_func_expr().unwrap();
            Ok(walk_opt(f.context_item, w)?
                || walk_opt(f.pathspec, w)?
                || walk_list(&f.passing, w)?
                || walk_opt(f.output, w)?
                || walk_opt(f.on_empty, w)?
                || walk_opt(f.on_error, w)?)
        }
        NodeTag::T_JsonTable => {
            let jt = node.as_json_table().unwrap();
            Ok(walk_opt(jt.context_item, w)?
                || walk_opt(jt.pathspec, w)?
                || walk_list(&jt.passing, w)?
                || walk_list(&jt.columns, w)?
                || walk_opt(jt.on_error, w)?)
        }
        NodeTag::T_JsonTableColumn => {
            let jtc = node.as_json_table_column().unwrap();
            Ok(walk_opt(jtc.typeName, w)?
                || walk_opt(jtc.on_empty, w)?
                || walk_opt(jtc.on_error, w)?
                || walk_list(&jtc.columns, w)?)
        }
        NodeTag::T_JsonTablePathSpec => {
            walk_opt(node.as_json_table_path_spec().unwrap().string, w)
        }
        NodeTag::T_JsonOutput => {
            let o = node.as_json_output().unwrap();
            walk_opt(o.typeName, w)
        }
        NodeTag::T_JsonKeyValue => {
            let kv = node.as_json_key_value().unwrap();
            Ok(walk_opt(kv.key, w)? || walk_opt(kv.value, w)?)
        }
        NodeTag::T_JsonObjectConstructor => {
            let c = node.as_json_object_constructor().unwrap();
            Ok(walk_opt(c.output, w)? || walk_list(&c.exprs, w)?)
        }
        NodeTag::T_JsonArrayConstructor => {
            let c = node.as_json_array_constructor().unwrap();
            Ok(walk_opt(c.output, w)? || walk_list(&c.exprs, w)?)
        }
        NodeTag::T_JsonAggConstructor => {
            let c = node.as_json_agg_constructor().unwrap();
            Ok(walk_opt(c.output, w)?
                || walk_list(&c.agg_order, w)?
                || walk_opt(c.agg_filter, w)?
                || walk_opt(c.over, w)?)
        }
        NodeTag::T_JsonObjectAgg => {
            let a = node.as_json_object_agg().unwrap();
            Ok(walk_opt(a.constructor, w)? || walk_opt(a.arg, w)?)
        }
        NodeTag::T_JsonArrayAgg => {
            let a = node.as_json_array_agg().unwrap();
            Ok(walk_opt(a.constructor, w)? || walk_opt(a.arg, w)?)
        }
        NodeTag::T_JsonArrayQueryConstructor => {
            let c = node.as_json_array_query_constructor().unwrap();
            Ok(walk_opt(c.output, w)? || walk_opt(c.query, w)?)
        }
        // Arms below follow C's raw_expression_tree_walker case order.
        NodeTag::T_GroupingFunc => walk_list(&node.as_grouping_func().unwrap().args, w),
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            // C: "we assume the operName is not interesting".
            Ok(walk_opt(sl.testexpr, w)? || w.visit(sl.subselect)?)
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if walk_opt(c.arg, w)? {
                return Ok(true);
            }
            // C: "we assume walker doesn't care about CaseWhens, either".
            for cell in &c.args {
                let cw = cell.as_case_when().expect("CaseExpr args are CaseWhen");
                if walk_opt(cw.expr, w)? || walk_opt(cw.result, w)? {
                    return Ok(true);
                }
            }
            walk_opt(c.defresult, w)
        }
        NodeTag::T_CoalesceExpr => walk_list(&node.as_coalesce_expr().unwrap().args, w),
        NodeTag::T_MinMaxExpr => walk_list(&node.as_min_max_expr().unwrap().args, w),
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            // C: "we assume walker doesn't care about arg_names".
            Ok(walk_list(&x.named_args, w)? || walk_list(&x.args, w)?)
        }
        NodeTag::T_NullTest => walk_opt(node.as_null_test().unwrap().arg, w),
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            // C: "using list is deemed uninteresting".
            Ok(w.visit(j.larg)?
                || w.visit(j.rarg)?
                || walk_opt(j.quals, w)?
                || match j.alias {
                    Some(a) => w.visit_alias_ref(a)?,
                    None => false,
                })
        }
        NodeTag::T_IntoClause => {
            let into = node.as_into_clause().unwrap();
            // C: "colNames, options are deemed uninteresting"; "viewQuery
            // should be null in raw parsetree, but check it".
            Ok(walk_opt(into.rel, w)? || walk_opt(into.viewQuery, w)?)
        }
        NodeTag::T_InsertStmt => {
            let s = node.as_insert_stmt().unwrap();
            Ok(walk_opt(s.relation, w)?
                || walk_list(&s.cols, w)?
                || walk_opt(s.selectStmt, w)?
                || walk_opt(s.onConflictClause, w)?
                || walk_opt(s.returningClause, w)?
                || walk_opt(s.withClause, w)?)
        }
        NodeTag::T_DeleteStmt => {
            let s = node.as_delete_stmt().unwrap();
            Ok(walk_opt(s.relation, w)?
                || walk_list(&s.usingClause, w)?
                || walk_opt(s.whereClause, w)?
                || walk_opt(s.returningClause, w)?
                || walk_opt(s.withClause, w)?)
        }
        NodeTag::T_UpdateStmt => {
            let s = node.as_update_stmt().unwrap();
            Ok(walk_opt(s.relation, w)?
                || walk_list(&s.targetList, w)?
                || walk_opt(s.whereClause, w)?
                || walk_list(&s.fromClause, w)?
                || walk_opt(s.returningClause, w)?
                || walk_opt(s.withClause, w)?)
        }
        NodeTag::T_MergeStmt => {
            let s = node.as_merge_stmt().unwrap();
            Ok(walk_opt(s.relation, w)?
                || walk_opt(s.sourceRelation, w)?
                || walk_opt(s.joinCondition, w)?
                || walk_list(&s.mergeWhenClauses, w)?
                || walk_opt(s.returningClause, w)?
                || walk_opt(s.withClause, w)?)
        }
        NodeTag::T_MergeWhenClause => {
            let m = node.as_merge_when_clause().unwrap();
            Ok(walk_opt(m.condition, w)?
                || walk_list(&m.targetList, w)?
                || walk_list(&m.values, w)?)
        }
        NodeTag::T_ReturningClause => {
            let r = node.as_returning_clause().unwrap();
            Ok(walk_list(&r.options, w)? || walk_list(&r.exprs, w)?)
        }
        NodeTag::T_PLAssignStmt => {
            let s = node.as_pl_assign_stmt().unwrap();
            Ok(walk_list(&s.indirection, w)? || walk_opt(s.val, w)?)
        }
        NodeTag::T_BoolExpr => walk_list(&node.as_bool_expr().unwrap().args, w),
        NodeTag::T_NamedArgExpr => walk_opt(node.as_named_arg_expr().unwrap().arg, w),
        NodeTag::T_MultiAssignRef => walk_opt(node.as_multi_assign_ref().unwrap().source, w),
        NodeTag::T_RangeSubselect => {
            let rs = node.as_range_subselect().unwrap();
            Ok(walk_opt(rs.subquery, w)?
                || match rs.alias {
                    Some(a) => w.visit_alias_ref(a)?,
                    None => false,
                })
        }
        NodeTag::T_RangeFunction => {
            let rf = node.as_range_function().unwrap();
            Ok(walk_list(&rf.functions, w)?
                || match rf.alias {
                    Some(a) => w.visit_alias_ref(a)?,
                    None => false,
                }
                || walk_list(&rf.coldeflist, w)?)
        }
        NodeTag::T_RangeTableSample => {
            let rts = node.as_range_table_sample().unwrap();
            // C: "method name is deemed uninteresting".
            Ok(walk_opt(rts.relation, w)?
                || walk_list(&rts.args, w)?
                || walk_opt(rts.repeatable, w)?)
        }
        NodeTag::T_ColumnDef => {
            let cd = node.as_column_def().unwrap();
            // C: "for now, constraints are ignored".
            Ok(walk_opt(cd.typeName, w)?
                || walk_opt(cd.raw_default, w)?
                || walk_opt(cd.collClause, w)?)
        }
        // C: "collation and opclass names are deemed uninteresting".
        NodeTag::T_IndexElem => walk_opt(node.as_index_elem().unwrap().expr, w),
        NodeTag::T_GroupingSet => walk_list(&node.as_grouping_set().unwrap().content, w),
        NodeTag::T_LockingClause => {
            walk_list(&node.as_locking_clause().unwrap().lockedRels, w)
        }
        NodeTag::T_WithClause => walk_list(&node.as_with_clause().unwrap().ctes, w),
        NodeTag::T_InferClause => {
            let ic = node.as_infer_clause().unwrap();
            Ok(walk_list(&ic.indexElems, w)? || walk_opt(ic.whereClause, w)?)
        }
        NodeTag::T_OnConflictClause => {
            let oc = node.as_on_conflict_clause().unwrap();
            Ok(walk_opt(oc.infer, w)?
                || walk_list(&oc.targetList, w)?
                || walk_opt(oc.whereClause, w)?)
        }
        // C: "search_clause and cycle_clause are not interesting here".
        NodeTag::T_CommonTableExpr => {
            walk_opt(node.as_common_table_expr().unwrap().ctequery, w)
        }
        other => deferred("raw_expression_tree_walker", other),
    }
}

// Closed-set exprType over CoerceViaIO's possible args.
fn coerce_io_arg_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        _ => expr_type(node),
    }
}

/// Apply `checker` to every function OID the node itself calls.
pub fn check_functions_in_node<'mcx, F>(node: Node<'mcx>, checker: &mut F) -> PgResult<bool>
where
    F: FnMut(Oid) -> PgResult<bool>,
{
    match node.node_tag() {
        NodeTag::T_Aggref => checker(node.as_aggref().unwrap().aggfnoid),
        NodeTag::T_FuncExpr => checker(node.as_func_expr().unwrap().funcid),
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // C set_opfuncid memo-writes into the node; sealed nodes are
            // immutable, so an unset opfuncid re-derives per visit (cold: the
            // parser fills it; only stored-rule trees arrive unset).
            let opfuncid = if o.opfuncid == 0 {
                lsyscache::operator::get_opcode(o.opno)?
            } else {
                o.opfuncid
            };
            checker(opfuncid)
        }
        NodeTag::T_WindowFunc => checker(node.as_window_func().unwrap().winfnoid),
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            let (infunc, _) = lsyscache::getTypeInputInfo(c.resulttype)?;
            if checker(infunc)? {
                return Ok(true);
            }
            let (outfunc, _) = lsyscache::getTypeOutputInfo(coerce_io_arg_type(c.arg))?;
            checker(outfunc)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            // set_sa_opfuncid, re-derived per visit as the OpExpr arm above.
            let opfuncid = if sa.opfuncid == 0 {
                lsyscache::operator::get_opcode(sa.opno)?
            } else {
                sa.opfuncid
            };
            checker(opfuncid)
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid = if d.opfuncid == 0 {
                lsyscache::operator::get_opcode(d.opno)?
            } else {
                d.opfuncid
            };
            checker(opfuncid)
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            let opfuncid = if d.opfuncid == 0 {
                lsyscache::operator::get_opcode(d.opno)?
            } else {
                d.opfuncid
            };
            checker(opfuncid)
        }
        NodeTag::T_RowCompareExpr => {
            for opno in &node.as_row_compare_expr().unwrap().opnos {
                if checker(lsyscache::operator::get_opcode(opno)?)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

/// Identity-preserving (module doc): `Ok(None)` = unchanged, share input.
/// C `strip_implicit_coercions` (nodeFuncs.c) over the ported coercion nodes;
/// unknown families return the node unchanged, as C.
pub fn strip_implicit_coercions(node: Node<'_>) -> Node<'_> {
    use types_nodes::primnodes::CoercionForm;
    match node.node_tag() {
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(f.args.nth(0));
            }
            node
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            if r.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(r.arg);
            }
            node
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            if c.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(c.arg);
            }
            node
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            if a.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(a.arg);
            }
            node
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            if c.convertformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(c.arg);
            }
            node
        }
        _ => node,
    }
}

/// Thin generic wrapper — coerces the caller's concrete `F` to a single
/// `&mut dyn FnMut` and delegates to the one monomorphic engine below. This is
/// the de-monomorphization "byte-shell" pattern (cf. 37f4f9def): N callsites
/// each stamp out this ~3-line shell instead of a full ~1,160-line copy of the
/// `NodeTag` match. Behavior is identical to a directly-generic body.
pub fn expression_tree_mutator<'mcx, F>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    m: &mut F,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    expression_tree_mutator_dyn(mcx, node, m)
}

/// Monomorphic engine: the callback is type-erased to `&mut dyn FnMut`, so this
/// large `NodeTag` match is codegen'd exactly once regardless of how many
/// distinct closures call it. Cold (planner/rewriter) path — the extra
/// indirect call per child is planning-time only, not per-row.
pub fn expression_tree_mutator_dyn<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    m: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_SortGroupClause => Ok(None),
        NodeTag::T_WithCheckOption => {
            let wco = node.as_with_check_option().unwrap();
            let qual = match wco.qual {
                Some(q) => m(q)?.map(Some),
                None => None,
            };
            match qual {
                None => Ok(None),
                Some(qual) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::parsenodes::WithCheckOption {
                        kind: wco.kind,
                        relname: wco.relname,
                        polname: wco.polname,
                        qual,
                        cascaded: wco.cascaded,
                    },
                )?)),
            }
        }
        NodeTag::T_CoerceToDomain => {
            let cd = node.as_coerce_to_domain().unwrap();
            match m(cd.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::CoerceToDomain {
                        arg,
                        resulttype: cd.resulttype,
                        resulttypmod: cd.resulttypmod,
                        resultcollid: cd.resultcollid,
                        coercionformat: cd.coercionformat,
                        location: cd.location,
                    },
                )?)),
            }
        }
        NodeTag::T_Aggref => {
            let a = node.as_variant::<Aggref>().unwrap();
            let args = mutate_list_dyn(mcx, &a.args, m)?;
            let directargs = mutate_list_dyn(mcx, &a.aggdirectargs, m)?;
            let aggorder = mutate_list_dyn(mcx, &a.aggorder, m)?;
            let aggdistinct = mutate_list_dyn(mcx, &a.aggdistinct, m)?;
            let aggfilter = match a.aggfilter {
                Some(f) => m(f)?.map(Some),
                None => None,
            };
            if args.is_none()
                && directargs.is_none()
                && aggorder.is_none()
                && aggdistinct.is_none()
                && aggfilter.is_none()
            {
                return Ok(None);
            }
            let unchanged = |new: Option<NodeList<'mcx>>, old: &NodeList<'mcx>| match new {
                Some(l) => Ok(l),
                None => old.clone_in(mcx),
            };
            Ok(Some(Node::mk(
                mcx,
                Aggref {
                    aggfnoid: a.aggfnoid,
                    aggtype: a.aggtype,
                    aggcollid: a.aggcollid,
                    inputcollid: a.inputcollid,
                    aggtranstype: a.aggtranstype,
                    aggargtypes: a.aggargtypes.clone_in(mcx)?,
                    aggdirectargs: unchanged(directargs, &a.aggdirectargs)?,
                    args: unchanged(args, &a.args)?,
                    aggorder: unchanged(aggorder, &a.aggorder)?,
                    aggdistinct: unchanged(aggdistinct, &a.aggdistinct)?,
                    aggfilter: aggfilter.unwrap_or(a.aggfilter),
                    aggstar: a.aggstar,
                    aggvariadic: a.aggvariadic,
                    aggkind: a.aggkind,
                    aggpresorted: a.aggpresorted,
                    agglevelsup: a.agglevelsup,
                    aggsplit: a.aggsplit,
                    aggno: a.aggno,
                    aggtransno: a.aggtransno,
                    location: a.location,
                },
            )?))
        }
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            match mutate_list_dyn(mcx, &g.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::GroupingFunc {
                        args,
                        refs: g.refs.clone_in(mcx)?,
                        cols: g.cols.clone_in(mcx)?,
                        agglevelsup: g.agglevelsup,
                        location: g.location,
                    },
                )?)),
            }
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            let args = mutate_list_dyn(mcx, &wf.args, m)?;
            let aggfilter = match wf.aggfilter {
                Some(f) => m(f)?.map(Some),
                None => None,
            };
            let run_condition = mutate_list_dyn(mcx, &wf.runCondition, m)?;
            if args.is_none() && aggfilter.is_none() && run_condition.is_none() {
                return Ok(None);
            }
            let unchanged = |new: Option<NodeList<'mcx>>, old: &NodeList<'mcx>| match new {
                Some(l) => Ok(l),
                None => old.clone_in(mcx),
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::WindowFunc {
                    winfnoid: wf.winfnoid,
                    wintype: wf.wintype,
                    wincollid: wf.wincollid,
                    inputcollid: wf.inputcollid,
                    args: unchanged(args, &wf.args)?,
                    aggfilter: aggfilter.unwrap_or(wf.aggfilter),
                    runCondition: unchanged(run_condition, &wf.runCondition)?,
                    winref: wf.winref,
                    winstar: wf.winstar,
                    winagg: wf.winagg,
                    location: wf.location,
                },
            )?))
        }
        NodeTag::T_WindowFuncRunCondition => {
            let rc = node.as_window_func_run_condition().unwrap();
            match m(rc.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::WindowFuncRunCondition {
                        opno: rc.opno,
                        inputcollid: rc.inputcollid,
                        wfunc_left: rc.wfunc_left,
                        arg,
                    },
                )?)),
            }
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            match mutate_list_dyn(mcx, &f.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    FuncExpr {
                        funcid: f.funcid,
                        funcresulttype: f.funcresulttype,
                        funcretset: f.funcretset,
                        funcvariadic: f.funcvariadic,
                        funcformat: f.funcformat,
                        funccollid: f.funccollid,
                        inputcollid: f.inputcollid,
                        args,
                        location: f.location,
                    },
                )?)),
            }
        }
        NodeTag::T_NamedArgExpr => {
            let na = node.as_named_arg_expr().unwrap();
            match m(na.arg.expect("NamedArgExpr has an arg"))? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::NamedArgExpr {
                        arg: Some(arg),
                        name: na.name,
                        argnumber: na.argnumber,
                        location: na.location,
                    },
                )?)),
            }
        }
        NodeTag::T_OpExpr => {
            let o = node.as_variant::<OpExpr>().unwrap();
            match mutate_list_dyn(mcx, &o.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    OpExpr {
                        opno: o.opno,
                        opfuncid: o.opfuncid,
                        opresulttype: o.opresulttype,
                        opretset: o.opretset,
                        opcollid: o.opcollid,
                        inputcollid: o.inputcollid,
                        args,
                        location: o.location,
                    },
                )?)),
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            match mutate_list_dyn(mcx, &sa.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::ScalarArrayOpExpr {
                        opno: sa.opno,
                        opfuncid: sa.opfuncid,
                        hashfuncid: sa.hashfuncid,
                        negfuncid: sa.negfuncid,
                        useOr: sa.useOr,
                        inputcollid: sa.inputcollid,
                        args,
                        location: sa.location,
                    },
                )?)),
            }
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            match mutate_list_dyn(mcx, &a.elements, m)? {
                None => Ok(None),
                Some(elements) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::ArrayExpr {
                        array_typeid: a.array_typeid,
                        array_collid: a.array_collid,
                        element_typeid: a.element_typeid,
                        elements,
                        multidims: a.multidims,
                        list_start: a.list_start,
                        list_end: a.list_end,
                        location: a.location,
                    },
                )?)),
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            let upper = mutate_opt_list_dyn(mcx, &sr.refupperindexpr, m)?;
            let lower = mutate_opt_list_dyn(mcx, &sr.reflowerindexpr, m)?;
            let refexpr = match sr.refexpr {
                Some(e) => m(e)?.map(Some),
                None => None,
            };
            let refassgn = match sr.refassgnexpr {
                Some(e) => m(e)?.map(Some),
                None => None,
            };
            if upper.is_none() && lower.is_none() && refexpr.is_none() && refassgn.is_none() {
                return Ok(None);
            }
            let unchanged = |new: Option<types_nodes::OptNodeList<'mcx>>,
                             old: &types_nodes::OptNodeList<'mcx>| match new {
                Some(l) => Ok(l),
                None => old.clone_in(mcx),
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::SubscriptingRef {
                    refcontainertype: sr.refcontainertype,
                    refelemtype: sr.refelemtype,
                    refrestype: sr.refrestype,
                    reftypmod: sr.reftypmod,
                    refcollid: sr.refcollid,
                    refupperindexpr: unchanged(upper, &sr.refupperindexpr)?,
                    reflowerindexpr: unchanged(lower, &sr.reflowerindexpr)?,
                    refexpr: refexpr.unwrap_or(sr.refexpr),
                    refassgnexpr: refassgn.unwrap_or(sr.refassgnexpr),
                },
            )?))
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match mutate_list_dyn(mcx, &b.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
                )?)),
            }
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            match m(r.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::RelabelType { arg, ..*r },
                )?)),
            }
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            match m(f.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::FieldSelect { arg, ..*f },
                )?)),
            }
        }
        NodeTag::T_ReturningExpr => {
            let r = node.as_returning_expr().unwrap();
            match m(r.retexpr)? {
                None => Ok(None),
                Some(retexpr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::ReturningExpr { retexpr, ..*r },
                )?)),
            }
        }
        NodeTag::T_TableSampleClause => {
            let tsc = node.as_table_sample_clause().unwrap();
            let args = mutate_list_dyn(mcx, &tsc.args, m)?;
            let repeatable = match tsc.repeatable {
                Some(r) => m(r)?.map(Some),
                None => None,
            };
            if args.is_none() && repeatable.is_none() {
                return Ok(None);
            }
            let args = match args {
                Some(l) => l,
                None => tsc.args.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::TableSampleClause {
                    tsmhandler: tsc.tsmhandler,
                    args,
                    repeatable: repeatable.unwrap_or(tsc.repeatable),
                },
            )?))
        }
        NodeTag::T_RangeTblFunction => {
            let rtf = node.as_range_tbl_function().unwrap();
            let funcexpr = match rtf.funcexpr {
                Some(f) => m(f)?.map(Some),
                None => None,
            };
            match funcexpr {
                None => Ok(None),
                Some(funcexpr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::RangeTblFunction {
                        funcexpr,
                        funccolcount: rtf.funccolcount,
                        funccolnames: rtf.funccolnames.clone_in(mcx)?,
                        funccoltypes: rtf.funccoltypes.clone_in(mcx)?,
                        funccoltypmods: rtf.funccoltypmods.clone_in(mcx)?,
                        funccolcollations: rtf.funccolcollations.clone_in(mcx)?,
                        funcparams: rtf.funcparams.clone_in(mcx)?,
                    },
                )?)),
            }
        }
        NodeTag::T_CollateExpr => {
            let c = node.as_collate_expr().unwrap();
            match m(c.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::CollateExpr { arg, ..*c },
                )?)),
            }
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            match m(c.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::CoerceViaIO { arg, ..*c },
                )?)),
            }
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            let arg = m(a.arg)?;
            let elemexpr = match a.elemexpr {
                Some(e) => m(e)?,
                None => None,
            };
            if arg.is_none() && elemexpr.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::ArrayCoerceExpr {
                    arg: arg.unwrap_or(a.arg),
                    elemexpr: elemexpr.or(a.elemexpr),
                    ..*a
                },
            )?))
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            match m(c.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::ConvertRowtypeExpr { arg, ..*c },
                )?)),
            }
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = match nt.arg {
                Some(a) => m(a)?,
                None => None,
            };
            match arg {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::NullTest {
                        arg: Some(arg),
                        nulltesttype: nt.nulltesttype,
                        argisrow: nt.argisrow,
                        location: nt.location,
                    },
                )?)),
            }
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            match mutate_list_dyn(mcx, &d.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::DistinctExpr {
                        opno: d.opno,
                        opfuncid: d.opfuncid,
                        opresulttype: d.opresulttype,
                        opretset: d.opretset,
                        opcollid: d.opcollid,
                        inputcollid: d.inputcollid,
                        args,
                        location: d.location,
                    },
                )?)),
            }
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            match mutate_list_dyn(mcx, &d.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::NullIfExpr {
                        opno: d.opno,
                        opfuncid: d.opfuncid,
                        opresulttype: d.opresulttype,
                        opretset: d.opretset,
                        opcollid: d.opcollid,
                        inputcollid: d.inputcollid,
                        args,
                        location: d.location,
                    },
                )?)),
            }
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().unwrap();
            let arg = match bt.arg {
                Some(a) => m(a)?,
                None => None,
            };
            match arg {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::BooleanTest {
                        arg: Some(arg),
                        booltesttype: bt.booltesttype,
                        location: bt.location,
                    },
                )?)),
            }
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            match mutate_list_dyn(mcx, &r.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::RowExpr {
                        args,
                        row_typeid: r.row_typeid,
                        row_format: r.row_format,
                        colnames: r.colnames.clone_in(mcx)?,
                        location: r.location,
                    },
                )?)),
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            let raw = mutate_opt_dyn(j.raw_expr, m)?;
            let formatted = mutate_opt_dyn(j.formatted_expr, m)?;
            if raw.is_none() && formatted.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::JsonValueExpr {
                    raw_expr: raw.or(j.raw_expr),
                    formatted_expr: formatted.or(j.formatted_expr),
                    format: j.format,
                },
            )?))
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            let args = mutate_list_dyn(mcx, &c.args, m)?;
            let func = mutate_opt_dyn(c.func, m)?;
            let coercion = mutate_opt_dyn(c.coercion, m)?;
            if args.is_none() && func.is_none() && coercion.is_none() {
                return Ok(None);
            }
            let args = match args {
                Some(l) => l,
                None => c.args.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::JsonConstructorExpr {
                    r#type: c.r#type,
                    args,
                    func: func.or(c.func),
                    coercion: coercion.or(c.coercion),
                    returning: c.returning,
                    absent_on_null: c.absent_on_null,
                    unique: c.unique,
                    location: c.location,
                },
            )?))
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().unwrap();
            match mutate_opt_dyn(p.expr, m)? {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::JsonIsPredicate {
                        expr: Some(expr),
                        format: p.format,
                        item_type: p.item_type,
                        unique_keys: p.unique_keys,
                        location: p.location,
                    },
                )?)),
            }
        }
        NodeTag::T_JsonBehavior => {
            let b = node.as_json_behavior().unwrap();
            match mutate_opt_dyn(b.expr, m)? {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::JsonBehavior {
                        btype: b.btype,
                        expr: Some(expr),
                        coerce: b.coerce,
                        location: b.location,
                    },
                )?)),
            }
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            let formatted = mutate_opt_dyn(j.formatted_expr, m)?;
            let path_spec = mutate_opt_dyn(j.path_spec, m)?;
            let passing_values = mutate_list_dyn(mcx, &j.passing_values, m)?;
            let on_empty = mutate_opt_dyn(j.on_empty, m)?;
            let on_error = mutate_opt_dyn(j.on_error, m)?;
            if formatted.is_none()
                && path_spec.is_none()
                && passing_values.is_none()
                && on_empty.is_none()
                && on_error.is_none()
            {
                return Ok(None);
            }
            let passing_values = match passing_values {
                Some(l) => l,
                None => j.passing_values.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::JsonExpr {
                    op: j.op,
                    column_name: j.column_name,
                    formatted_expr: formatted.or(j.formatted_expr),
                    format: j.format,
                    path_spec: path_spec.or(j.path_spec),
                    returning: j.returning,
                    passing_names: j.passing_names.clone_in(mcx)?,
                    passing_values,
                    on_empty: on_empty.or(j.on_empty),
                    on_error: on_error.or(j.on_error),
                    use_io_coercion: j.use_io_coercion,
                    use_json_coercion: j.use_json_coercion,
                    wrapper: j.wrapper,
                    omit_quotes: j.omit_quotes,
                    collation: j.collation,
                    location: j.location,
                },
            )?))
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let largs = mutate_list_dyn(mcx, &rc.largs, m)?;
            let rargs = mutate_list_dyn(mcx, &rc.rargs, m)?;
            if largs.is_none() && rargs.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(mcx)?,
                    opfamilies: rc.opfamilies.clone_in(mcx)?,
                    inputcollids: rc.inputcollids.clone_in(mcx)?,
                    largs: match largs {
                        Some(l) => l,
                        None => rc.largs.clone_in(mcx)?,
                    },
                    rargs: match rargs {
                        Some(l) => l,
                        None => rc.rargs.clone_in(mcx)?,
                    },
                },
            )?))
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            let arg = m(fs.arg)?;
            let newvals = mutate_list_dyn(mcx, &fs.newvals, m)?;
            if arg.is_none() && newvals.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::FieldStore {
                    arg: match arg {
                        Some(a) => a,
                        None => fs.arg,
                    },
                    newvals: match newvals {
                        Some(l) => l,
                        None => fs.newvals.clone_in(mcx)?,
                    },
                    fieldnums: fs.fieldnums.clone_in(mcx)?,
                    resulttype: fs.resulttype,
                },
            )?))
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            match mutate_list_dyn(mcx, &mm.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    MinMaxExpr {
                        minmaxtype: mm.minmaxtype,
                        minmaxcollid: mm.minmaxcollid,
                        inputcollid: mm.inputcollid,
                        op: mm.op,
                        args,
                        location: mm.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            match mutate_list_dyn(mcx, &co.args, m)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::CoalesceExpr {
                        coalescetype: co.coalescetype,
                        coalescecollid: co.coalescecollid,
                        args,
                        location: co.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => m(a)?.map(Some),
                None => None,
            };
            let args = mutate_list_dyn(mcx, &c.args, m)?;
            let defresult = match c.defresult {
                Some(d) => m(d)?.map(Some),
                None => None,
            };
            if arg.is_none() && args.is_none() && defresult.is_none() {
                return Ok(None);
            }
            let args = match args {
                Some(l) => l,
                None => c.args.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg: arg.unwrap_or(c.arg),
                    args,
                    defresult: defresult.unwrap_or(c.defresult),
                    location: c.location,
                },
            )?))
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            let expr = match cw.expr {
                Some(e) => m(e)?.map(Some),
                None => None,
            };
            let result = match cw.result {
                Some(r) => m(r)?.map(Some),
                None => None,
            };
            if expr.is_none() && result.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::CaseWhen {
                    expr: expr.unwrap_or(cw.expr),
                    result: result.unwrap_or(cw.result),
                    location: cw.location,
                },
            )?))
        }
        NodeTag::T_TargetEntry => {
            let te = node.as_variant::<TargetEntry>().unwrap();
            match m(te.expr)? {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    TargetEntry {
                        expr,
                        resno: te.resno,
                        resname: te.resname,
                        ressortgroupref: te.ressortgroupref,
                        resorigtbl: te.resorigtbl,
                        resorigcol: te.resorigcol,
                        resjunk: te.resjunk,
                    },
                )?)),
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            let fromlist = mutate_list_dyn(mcx, &f.fromlist, m)?;
            let quals = match f.quals {
                Some(q) => m(q)?.map(Some),
                None => None,
            };
            if fromlist.is_none() && quals.is_none() {
                return Ok(None);
            }
            let fromlist = match fromlist {
                Some(l) => l,
                None => f.fromlist.clone_in(mcx)?,
            };
            Ok(Some(Node::mk(
                mcx,
                FromExpr { fromlist, quals: quals.unwrap_or(f.quals) },
            )?))
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = m(j.larg)?;
            let rarg = m(j.rarg)?;
            let quals = match j.quals {
                Some(q) => m(q)?.map(Some),
                None => None,
            };
            if larg.is_none() && rarg.is_none() && quals.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg: larg.unwrap_or(j.larg),
                    rarg: rarg.unwrap_or(j.rarg),
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: quals.unwrap_or(j.quals),
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )?))
        }
        NodeTag::T_List => match mutate_list_dyn(mcx, node.as_list().unwrap(), m)? {
            None => Ok(None),
            Some(l) => Ok(Some(Node::mk_list(mcx, l)?)),
        },
        NodeTag::T_SetOperationStmt => {
            // C mutates larg/rarg/groupClauses; the col* value lists are
            // copied (nodeFuncs.c expression_tree_mutator).
            let so = node.as_set_operation_stmt().unwrap();
            let larg = mutate_opt_dyn(so.larg, m)?;
            let rarg = mutate_opt_dyn(so.rarg, m)?;
            let group_clauses = mutate_list_dyn(mcx, &so.groupClauses, m)?;
            if larg.is_none() && rarg.is_none() && group_clauses.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::parsenodes::SetOperationStmt {
                    op: so.op,
                    all: so.all,
                    larg: larg.or(so.larg),
                    rarg: rarg.or(so.rarg),
                    colTypes: types_nodes::OidList::from_slice(mcx, so.colTypes.as_slice())?,
                    colTypmods: types_nodes::IntList::from_slice(mcx, so.colTypmods.as_slice())?,
                    colCollations: types_nodes::OidList::from_slice(mcx, so.colCollations.as_slice())?,
                    groupClauses: match group_clauses {
                        Some(g) => g,
                        None => so.groupClauses.clone_in(mcx)?,
                    },
                },
            )?))
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            match m(phv.phexpr)? {
                None => Ok(None),
                Some(new_expr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::PlaceHolderVar {
                        phexpr: new_expr,
                        phrels: phv.phrels.clone_in(mcx)?,
                        phnullingrels: phv.phnullingrels.clone_in(mcx)?,
                        phid: phv.phid,
                        phlevelsup: phv.phlevelsup,
                    },
                )?)),
            }
        }
        // C mutates testexpr and args; the child Plan tree is not expression
        // territory.
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let new_te = match sp.testexpr {
                None => None,
                Some(te) => m(te)?,
            };
            let new_args = mutate_list_dyn(mcx, &sp.args, m)?;
            if new_te.is_none() && new_args.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr: new_te.or(sp.testexpr),
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args: match new_args {
                        Some(l) => l,
                        None => sp.args.clone_in(mcx)?,
                    },
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )?))
        }
        // C mutates testexpr only; the subselect Query is shared untouched.
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            match sl.testexpr {
                None => Ok(None),
                Some(te) => match m(te)? {
                    None => Ok(None),
                    Some(new_te) => Ok(Some(Node::mk(
                        mcx,
                        types_nodes::SubLink {
                            subLinkType: sl.subLinkType,
                            subLinkId: sl.subLinkId,
                            testexpr: Some(new_te),
                            operName: sl.operName.clone_in(mcx)?,
                            subselect: sl.subselect,
                            location: sl.location,
                        },
                    )?)),
                },
            }
        }
        NodeTag::T_MergeAction => {
            let a = node.as_merge_action().unwrap();
            let new_qual = match a.qual {
                Some(q) => m(q)?,
                None => None,
            };
            let new_tl = mutate_list_dyn(mcx, &a.targetList, m)?;
            if new_qual.is_none() && new_tl.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::MergeAction {
                    matchKind: a.matchKind,
                    commandType: a.commandType,
                    r#override: a.r#override,
                    qual: match new_qual {
                        Some(q) => Some(q),
                        None => a.qual,
                    },
                    targetList: match new_tl {
                        Some(l) => l,
                        None => a.targetList.clone_in(mcx)?,
                    },
                    updateColnos: a.updateColnos.clone_in(mcx)?,
                },
            )?))
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            let named_args = mutate_list_dyn(mcx, &x.named_args, m)?;
            let args = mutate_list_dyn(mcx, &x.args, m)?;
            if named_args.is_none() && args.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::XmlExpr {
                    op: x.op,
                    name: x.name,
                    named_args: match named_args {
                        Some(l) => l,
                        None => x.named_args.clone_in(mcx)?,
                    },
                    arg_names: x.arg_names.clone_in(mcx)?,
                    args: match args {
                        Some(l) => l,
                        None => x.args.clone_in(mcx)?,
                    },
                    xmloption: x.xmloption,
                    indent: x.indent,
                    r#type: x.r#type,
                    typmod: x.typmod,
                    location: x.location,
                },
            )?))
        }
        NodeTag::T_TableFunc => {
            let tf = node.as_table_func().unwrap();
            let ns_uris = mutate_list_dyn(mcx, &tf.ns_uris, m)?;
            let docexpr = match tf.docexpr {
                Some(d) => m(d)?.map(Some),
                None => None,
            };
            let rowexpr = match tf.rowexpr {
                Some(r) => m(r)?.map(Some),
                None => None,
            };
            let colexprs = mutate_opt_list_dyn(mcx, &tf.colexprs, m)?;
            let coldefexprs = mutate_opt_list_dyn(mcx, &tf.coldefexprs, m)?;
            let colvalexprs = mutate_opt_list_dyn(mcx, &tf.colvalexprs, m)?;
            let passingvalexprs = mutate_list_dyn(mcx, &tf.passingvalexprs, m)?;
            if ns_uris.is_none()
                && docexpr.is_none()
                && rowexpr.is_none()
                && colexprs.is_none()
                && coldefexprs.is_none()
                && colvalexprs.is_none()
                && passingvalexprs.is_none()
            {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::TableFunc {
                    functype: tf.functype,
                    ns_uris: match ns_uris {
                        Some(l) => l,
                        None => tf.ns_uris.clone_in(mcx)?,
                    },
                    ns_names: tf.ns_names.clone_in(mcx)?,
                    docexpr: docexpr.unwrap_or(tf.docexpr),
                    rowexpr: rowexpr.unwrap_or(tf.rowexpr),
                    colnames: tf.colnames.clone_in(mcx)?,
                    coltypes: tf.coltypes.clone_in(mcx)?,
                    coltypmods: tf.coltypmods.clone_in(mcx)?,
                    colcollations: tf.colcollations.clone_in(mcx)?,
                    colexprs: match colexprs {
                        Some(l) => l,
                        None => tf.colexprs.clone_in(mcx)?,
                    },
                    coldefexprs: match coldefexprs {
                        Some(l) => l,
                        None => tf.coldefexprs.clone_in(mcx)?,
                    },
                    colvalexprs: match colvalexprs {
                        Some(l) => l,
                        None => tf.colvalexprs.clone_in(mcx)?,
                    },
                    passingvalexprs: match passingvalexprs {
                        Some(l) => l,
                        None => tf.passingvalexprs.clone_in(mcx)?,
                    },
                    notnulls: tf.notnulls.clone_in(mcx)?,
                    plan: tf.plan,
                    ordinalitycol: tf.ordinalitycol,
                    location: tf.location,
                },
            )?))
        }
        NodeTag::T_InferenceElem => {
            let ie = node.as_inference_elem().unwrap();
            let expr = match ie.expr {
                Some(e) => m(e)?.map(Some),
                None => None,
            };
            match expr {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::InferenceElem {
                        expr,
                        infercollid: ie.infercollid,
                        inferopclass: ie.inferopclass,
                    },
                )?)),
            }
        }
        NodeTag::T_OnConflictExpr => {
            let oc = node.as_on_conflict_expr().unwrap();
            let arbiter_elems = mutate_list_dyn(mcx, &oc.arbiterElems, m)?;
            let arbiter_where = mutate_opt_dyn(oc.arbiterWhere, m)?;
            let on_conflict_set = mutate_list_dyn(mcx, &oc.onConflictSet, m)?;
            let on_conflict_where = mutate_opt_dyn(oc.onConflictWhere, m)?;
            let excl_rel_tlist = mutate_list_dyn(mcx, &oc.exclRelTlist, m)?;
            if arbiter_elems.is_none()
                && arbiter_where.is_none()
                && on_conflict_set.is_none()
                && on_conflict_where.is_none()
                && excl_rel_tlist.is_none()
            {
                return Ok(None);
            }
            let unchanged = |new: Option<NodeList<'mcx>>, old: &NodeList<'mcx>| match new {
                Some(l) => Ok(l),
                None => old.clone_in(mcx),
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::OnConflictExpr {
                    action: oc.action,
                    arbiterElems: unchanged(arbiter_elems, &oc.arbiterElems)?,
                    arbiterWhere: arbiter_where.or(oc.arbiterWhere),
                    constraint: oc.constraint,
                    onConflictSet: unchanged(on_conflict_set, &oc.onConflictSet)?,
                    onConflictWhere: on_conflict_where.or(oc.onConflictWhere),
                    exclRelIndex: oc.exclRelIndex,
                    exclRelTlist: unchanged(excl_rel_tlist, &oc.exclRelTlist)?,
                },
            )?))
        }
        NodeTag::T_AlternativeSubPlan => {
            let asp = node.as_alternative_sub_plan().unwrap();
            match mutate_list_dyn(mcx, &asp.subplans, m)? {
                None => Ok(None),
                Some(subplans) => Ok(Some(Node::mk(
                    mcx,
                    types_nodes::primnodes::AlternativeSubPlan { subplans },
                )?)),
            }
        }
        NodeTag::T_PartitionBoundSpec => {
            let pbs = node.as_variant::<types_nodes::rawnodes::PartitionBoundSpec>().unwrap();
            let listdatums = mutate_list_dyn(mcx, &pbs.listdatums, m)?;
            let lowerdatums = mutate_list_dyn(mcx, &pbs.lowerdatums, m)?;
            let upperdatums = mutate_list_dyn(mcx, &pbs.upperdatums, m)?;
            if listdatums.is_none() && lowerdatums.is_none() && upperdatums.is_none() {
                return Ok(None);
            }
            let unchanged = |new: Option<NodeList<'mcx>>, old: &NodeList<'mcx>| match new {
                Some(l) => Ok(l),
                None => old.clone_in(mcx),
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::rawnodes::PartitionBoundSpec {
                    strategy: pbs.strategy,
                    is_default: pbs.is_default,
                    modulus: pbs.modulus,
                    remainder: pbs.remainder,
                    listdatums: unchanged(listdatums, &pbs.listdatums)?,
                    lowerdatums: unchanged(lowerdatums, &pbs.lowerdatums)?,
                    upperdatums: unchanged(upperdatums, &pbs.upperdatums)?,
                    location: pbs.location,
                },
            )?))
        }
        // Range-bound list elements: MINVALUE/MAXVALUE carry no value node.
        NodeTag::T_PartitionRangeDatum => {
            let prd = node.as_variant::<types_nodes::rawnodes::PartitionRangeDatum>().unwrap();
            let Some(value) = mutate_opt_dyn(prd.value, m)? else {
                return Ok(None);
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::rawnodes::PartitionRangeDatum {
                    kind: prd.kind,
                    value: Some(value),
                    location: prd.location,
                },
            )?))
        }
        other => deferred("expression_tree_mutator", other),
    }
}

/// Mutate an optional child; `None` = unchanged (absent child included).
/// Generic wrapper over the erased-callback engine (de-mono byte-shell).
pub fn mutate_opt<'mcx, F>(
    n: Option<Node<'mcx>>,
    m: &mut F,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    mutate_opt_dyn(n, m)
}

pub fn mutate_opt_dyn<'mcx>(
    n: Option<Node<'mcx>>,
    m: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<Node<'mcx>>> {
    match n {
        Some(x) => m(x),
        None => Ok(None),
    }
}

/// Element-wise mutate; allocates a new list only after the first change.
/// Generic wrapper over the erased-callback engine (de-mono byte-shell).
pub fn mutate_opt_list<'mcx, F>(
    mcx: Mcx<'mcx>,
    list: &types_nodes::OptNodeList<'mcx>,
    m: &mut F,
) -> PgResult<Option<types_nodes::OptNodeList<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    mutate_opt_list_dyn(mcx, list, m)
}

pub fn mutate_opt_list_dyn<'mcx>(
    mcx: Mcx<'mcx>,
    list: &types_nodes::OptNodeList<'mcx>,
    m: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<types_nodes::OptNodeList<'mcx>>> {
    let mut out: Option<types_nodes::OptNodeList<'mcx>> = None;
    for (i, n) in list.iter().enumerate() {
        let Some(n) = n else { continue };
        let replaced = m(n)?;
        if replaced.is_some() && out.is_none() {
            out = Some(list.clone_in(mcx)?);
        }
        if let (Some(new), Some(l)) = (replaced, out.as_mut()) {
            l.as_mut_slice()[i] = Some(new);
        }
    }
    Ok(out)
}

/// Generic wrapper over the erased-callback engine (de-mono byte-shell).
pub fn mutate_list<'mcx, F>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    m: &mut F,
) -> PgResult<Option<NodeList<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    mutate_list_dyn(mcx, list, m)
}

pub fn mutate_list_dyn<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    m: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut out: Option<NodeList<'mcx>> = None;
    for (i, n) in list.iter().enumerate() {
        let replaced = m(n)?;
        if replaced.is_some() && out.is_none() {
            out = Some(list.clone_in(mcx)?);
        }
        if let (Some(new), Some(l)) = (replaced, out.as_mut()) {
            l.as_mut_slice()[i] = new;
        }
    }
    Ok(out)
}

/// C fix_opfuncids (nodeFuncs.c): planned-expression invariant that every
/// OpExpr carries its opfuncid (readfuncs trees arrive filled; a zero memo
/// is re-derived in place).
pub fn fix_opfuncids(node: Node<'_>) -> PgResult<()> {
    struct W;
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_OpExpr {
                let o = node.as_variant::<OpExpr>().unwrap();
                if o.opfuncid == 0 {
                    let opfuncid = lsyscache::operator::get_opcode(o.opno)?;
                    // SAFETY: fix_opfuncids callers hold the just-read tree
                    // exclusively; the shared borrow above has ended.
                    unsafe {
                        node.with_mut::<OpExpr, _>(|o| o.opfuncid = opfuncid).unwrap();
                    }
                }
            }
            expression_tree_walker(node, self)
        }
    }
    W.visit(node).map(|_| ())
}
