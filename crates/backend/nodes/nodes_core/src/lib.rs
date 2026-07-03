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

pub mod node_funcs;
pub use node_funcs::{
    expr_collation, expr_is_null_constant, expr_location, expr_type, expr_typmod,
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

pub fn expression_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
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
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            walk_list(&f.args, w)
        }
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
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            walk_list(&b.args, w)
        }
        NodeTag::T_NullTest => walk_opt(node.as_null_test().unwrap().arg, w),
        NodeTag::T_RelabelType => w.visit(node.as_relabel_type().unwrap().arg),
        NodeTag::T_CollateExpr => w.visit(node.as_collate_expr().unwrap().arg),
        NodeTag::T_CoerceViaIO => w.visit(node.as_coerce_via_io().unwrap().arg),
        NodeTag::T_BooleanTest => walk_opt(node.as_boolean_test().unwrap().arg, w),
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
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
        NodeTag::T_CoerceToDomain => w.visit(node.as_coerce_to_domain().unwrap().arg),
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            walk_list(&mm.args, w)
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
        NodeTag::T_List => walk_list(node.as_list().unwrap(), w),
        NodeTag::T_RangeTblFunction => {
            walk_opt(node.as_range_tbl_function().unwrap().funcexpr, w)
        }
        other => deferred("expression_tree_walker", other),
    }
}

pub fn query_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    query: &'mcx Query<'mcx>,
    w: &mut W,
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
    if flags & QTW_IGNORE_RANGE_TABLE == 0 && range_table_walker(&query.rtable, w, flags)? {
        return Ok(true);
    }
    Ok(false)
}

pub fn range_table_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    rtable: &NodeList<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    for rte in rtable {
        if range_table_entry_walker(rte, w, flags)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn range_table_entry_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    rte_node: Node<'mcx>,
    w: &mut W,
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

pub fn query_or_expression_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
    flags: u32,
) -> PgResult<bool> {
    match node.as_query() {
        Some(q) => query_tree_walker(q, w, flags),
        None => w.visit(node),
    }
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

pub fn raw_expression_tree_walker<'mcx, W: NodeWalker<'mcx> + ?Sized>(
    node: Node<'mcx>,
    w: &mut W,
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
        | NodeTag::T_MergeSupportFunc => Ok(false),
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
        NodeTag::T_ResTarget => {
            let rt = node.as_res_target().unwrap();
            Ok(walk_list(&rt.indirection, w)? || walk_opt(rt.val, w)?)
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
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        NodeTag::T_JsonConstructorExpr => {
            node.as_json_constructor_expr().unwrap().returning.expect("returning").typid
        }
        other => deferred("coerce_io_arg_type (exprType)", other),
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
        t @ (NodeTag::T_NullIfExpr
        | NodeTag::T_RowCompareExpr) => deferred("check_functions_in_node", t),
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
        _ => node,
    }
}

pub fn expression_tree_mutator<'mcx, F>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    m: &mut F,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
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
        | NodeTag::T_SortGroupClause => Ok(None),
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
            let args = mutate_list(mcx, &a.args, m)?;
            let directargs = mutate_list(mcx, &a.aggdirectargs, m)?;
            let aggorder = mutate_list(mcx, &a.aggorder, m)?;
            let aggdistinct = mutate_list(mcx, &a.aggdistinct, m)?;
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
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            let args = mutate_list(mcx, &wf.args, m)?;
            let aggfilter = match wf.aggfilter {
                Some(f) => m(f)?.map(Some),
                None => None,
            };
            let run_condition = mutate_list(mcx, &wf.runCondition, m)?;
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
        NodeTag::T_FuncExpr => {
            let f = node.as_variant::<FuncExpr>().unwrap();
            match mutate_list(mcx, &f.args, m)? {
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
        NodeTag::T_OpExpr => {
            let o = node.as_variant::<OpExpr>().unwrap();
            match mutate_list(mcx, &o.args, m)? {
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
            match mutate_list(mcx, &sa.args, m)? {
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
            match mutate_list(mcx, &a.elements, m)? {
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
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match mutate_list(mcx, &b.args, m)? {
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
            match mutate_list(mcx, &d.args, m)? {
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
            match mutate_list(mcx, &r.args, m)? {
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
            let raw = mutate_opt(j.raw_expr, m)?;
            let formatted = mutate_opt(j.formatted_expr, m)?;
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
            let args = mutate_list(mcx, &c.args, m)?;
            let func = mutate_opt(c.func, m)?;
            let coercion = mutate_opt(c.coercion, m)?;
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
            match mutate_opt(p.expr, m)? {
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
            match mutate_opt(b.expr, m)? {
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
            let formatted = mutate_opt(j.formatted_expr, m)?;
            let path_spec = mutate_opt(j.path_spec, m)?;
            let passing_values = mutate_list(mcx, &j.passing_values, m)?;
            let on_empty = mutate_opt(j.on_empty, m)?;
            let on_error = mutate_opt(j.on_error, m)?;
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
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            match mutate_list(mcx, &mm.args, m)? {
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
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => m(a)?.map(Some),
                None => None,
            };
            let args = mutate_list(mcx, &c.args, m)?;
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
            let fromlist = mutate_list(mcx, &f.fromlist, m)?;
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
        NodeTag::T_List => match mutate_list(mcx, node.as_list().unwrap(), m)? {
            None => Ok(None),
            Some(l) => Ok(Some(Node::mk_list(mcx, l)?)),
        },
        // C mutates testexpr and args; the child Plan tree is not expression
        // territory.
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let new_te = match sp.testexpr {
                None => None,
                Some(te) => m(te)?,
            };
            let new_args = mutate_list(mcx, &sp.args, m)?;
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
        other => deferred("expression_tree_mutator", other),
    }
}

/// Mutate an optional child; `None` = unchanged (absent child included).
pub fn mutate_opt<'mcx, F>(
    n: Option<Node<'mcx>>,
    m: &mut F,
) -> PgResult<Option<Node<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
    match n {
        Some(x) => m(x),
        None => Ok(None),
    }
}

/// Element-wise mutate; allocates a new list only after the first change.
pub fn mutate_list<'mcx, F>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    m: &mut F,
) -> PgResult<Option<NodeList<'mcx>>>
where
    F: FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
{
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
