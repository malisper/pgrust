// find_expr_references_walker slice bounded to the node set a view SELECT
// produces; every other tag/rtekind/Query feature is loud with its C symbol.
use cache_syscache::SysCacheKey;
use datum::Datum;
use mcx::Mcx;
use pg_depend::{object_address_comparator, DependencyType, ObjectAddress};
use types_core::{
    catalog::{DEFAULT_COLLATION_OID, RECORDOID},
    InvalidOid, Oid, CONSTRAINT_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::list::NodeList;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry, RangeTblFunction};
use types_nodes::NodeTag;

const OperatorRelationId: Oid = 2617;
const OperatorFamilyRelationId: Oid = 2753;
const CollationRelationId: Oid = 3456;
const TSConfigRelationId: Oid = 3602;
const TSDictionaryRelationId: Oid = 3600;
const InvalidAttrNumber: i16 = 0;

#[cold]
#[inline(never)]
fn walker_unported(what: &str) -> ! {
    panic!("unported: dependency.c find_expr_references_walker {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn walker_error(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

struct FindExprContext<'w, 'mcx> {
    addrs: Vec<ObjectAddress>,
    rtables: Vec<&'w NodeList<'mcx>>,
}

impl FindExprContext<'_, '_> {
    fn add(&mut self, class_id: Oid, object_id: Oid, sub_id: i32) {
        self.addrs.push(ObjectAddress::sub_set(class_id, object_id, sub_id));
    }
}

pub fn recordDependencyOnExpr<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    expr: Node<'mcx>,
    rtable: &NodeList<'mcx>,
    behavior: DependencyType,
) -> PgResult<()> {
    let refs = find_expr_references(expr, rtable)?;
    pg_depend::recordMultipleDependencies(mcx, depender, &refs, behavior)
}

pub fn find_expr_references<'mcx>(
    expr: Node<'mcx>,
    rtable: &NodeList<'mcx>,
) -> PgResult<Vec<ObjectAddress>> {
    let mut context = FindExprContext { addrs: Vec::new(), rtables: vec![rtable] };
    walker(expr, &mut context)?;
    let mut addrs = context.addrs;
    eliminate_duplicate_dependencies(&mut addrs);
    Ok(addrs)
}

pub fn eliminate_duplicate_dependencies(addrs: &mut Vec<ObjectAddress>) {
    if addrs.len() <= 1 {
        return;
    }
    addrs.sort_by(object_address_comparator);
    let mut prior = 0;
    for oldref in 1..addrs.len() {
        let thisobj = addrs[oldref];
        if addrs[prior].classId == thisobj.classId
            && addrs[prior].objectId == thisobj.objectId
        {
            if addrs[prior].objectSubId == thisobj.objectSubId {
                continue;
            }
            // A whole-object ref plus a column ref of the same object keeps
            // only the column ref; whole-object sorts first (subId 0).
            if addrs[prior].objectSubId == 0 {
                addrs[prior].objectSubId = thisobj.objectSubId;
                continue;
            }
        }
        prior += 1;
        addrs[prior] = thisobj;
    }
    addrs.truncate(prior + 1);
}

fn walk_list<'w, 'mcx: 'w>(
    list: &NodeList<'mcx>,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    for item in list {
        walker(item, context)?;
    }
    Ok(())
}

fn walk_opt<'w, 'mcx: 'w>(
    node: Option<Node<'mcx>>,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    match node {
        Some(n) => walker(n, context),
        None => Ok(()),
    }
}

fn walker<'w, 'mcx: 'w>(
    node: Node<'mcx>,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            let lvl = var.varlevelsup as usize;
            if lvl >= context.rtables.len() {
                return Err(walker_error(format!(
                    "invalid varlevelsup {}",
                    var.varlevelsup
                )));
            }
            let rtable = context.rtables[lvl];
            if var.varno <= 0 || var.varno as usize > rtable.len() {
                return Err(walker_error(format!("invalid varno {}", var.varno)));
            }
            let rte = rtable
                .nth(var.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry is a RangeTblEntry");
            // A whole-row Var adds nothing: the whole-table dependency comes
            // from the rangetable entry.
            if var.varattno == InvalidAttrNumber {
                return Ok(());
            }
            match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    context.add(RELATION_RELATION_ID, rte.relid, var.varattno as i32);
                }
                RTEKind::RTE_FUNCTION => {
                    process_function_rte_ref(rte, var.varattno, context)?;
                }
                // Join alias Vars reference merged USING columns whose inputs
                // are covered via the join quals; subquery Vars via recursion.
                _ => {}
            }
            Ok(())
        }
        NodeTag::T_Const => {
            let con = node.as_const().unwrap();
            context.add(TYPE_RELATION_ID, con.consttype, 0);
            if con.constcollid != InvalidOid && con.constcollid != DEFAULT_COLLATION_OID {
                context.add(CollationRelationId, con.constcollid, 0);
            }
            if !con.constisnull {
                let objoid = con.constvalue.as_oid();
                match con.consttype {
                    24 | 2202 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::PROCOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(types_core::PROCEDURE_RELATION_ID, objoid, 0);
                        }
                    }
                    2203 | 2204 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::OPEROID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(OperatorRelationId, objoid, 0);
                        }
                    }
                    2205 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::RELOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(RELATION_RELATION_ID, objoid, 0);
                        }
                    }
                    2206 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::TYPEOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(TYPE_RELATION_ID, objoid, 0);
                        }
                    }
                    4191 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::COLLOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(CollationRelationId, objoid, 0);
                        }
                    }
                    3734 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::TSCONFIGOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(TSConfigRelationId, objoid, 0);
                        }
                    }
                    3769 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::TSDICTOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(TSDictionaryRelationId, objoid, 0);
                        }
                    }
                    4089 => {
                        if cache_syscache::SearchSysCacheExists(
                            cache_syscache::NAMESPACEOID,
                            SysCacheKey::Value(Datum::from_oid(objoid)),
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                            SysCacheKey::UNUSED,
                        )? {
                            context.add(types_core::NAMESPACE_RELATION_ID, objoid, 0);
                        }
                    }
                    4096 => {
                        return Err(Box::new(
                            PgError::error(
                                "constant of the type regrole cannot be used here".to_string(),
                            )
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let funcexpr = node.as_func_expr().unwrap();
            context.add(types_core::PROCEDURE_RELATION_ID, funcexpr.funcid, 0);
            walk_list(&funcexpr.args, context)
        }
        NodeTag::T_TableSampleClause => {
            let tsc = node.as_table_sample_clause().unwrap();
            context.add(types_core::PROCEDURE_RELATION_ID, tsc.tsmhandler, 0);
            walk_list(&tsc.args, context)?;
            walk_opt(tsc.repeatable, context)
        }
        NodeTag::T_OpExpr => {
            let opexpr = node.as_op_expr().unwrap();
            context.add(OperatorRelationId, opexpr.opno, 0);
            walk_list(&opexpr.args, context)
        }
        NodeTag::T_DistinctExpr => {
            let opexpr = node.as_distinct_expr().unwrap();
            context.add(OperatorRelationId, opexpr.opno, 0);
            walk_list(&opexpr.args, context)
        }
        NodeTag::T_NullIfExpr => {
            let opexpr = node.as_null_if_expr().unwrap();
            context.add(OperatorRelationId, opexpr.opno, 0);
            walk_list(&opexpr.args, context)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let opexpr = node.as_scalar_array_op_expr().unwrap();
            context.add(OperatorRelationId, opexpr.opno, 0);
            walk_list(&opexpr.args, context)
        }
        NodeTag::T_WindowFunc => {
            let wfunc = node.as_window_func().unwrap();
            context.add(types_core::PROCEDURE_RELATION_ID, wfunc.winfnoid, 0);
            walk_list(&wfunc.args, context)?;
            walk_opt(wfunc.aggfilter, context)?;
            walk_list(&wfunc.runCondition, context)
        }
        NodeTag::T_SubscriptingRef => {
            let sbsref = node.as_subscripting_ref().unwrap();
            // Only a custom handler's refrestype needs its own dep; the
            // container/element types ride with refexpr.
            if sbsref.refrestype != sbsref.refcontainertype
                && sbsref.refrestype != sbsref.refelemtype
            {
                context.add(TYPE_RELATION_ID, sbsref.refrestype, 0);
            }
            for e in sbsref.refupperindexpr.iter() {
                walk_opt(e, context)?;
            }
            for e in sbsref.reflowerindexpr.iter() {
                walk_opt(e, context)?;
            }
            walk_opt(sbsref.refexpr, context)?;
            walk_opt(sbsref.refassgnexpr, context)
        }
        NodeTag::T_SubPlan => {
            Err(walker_error("already-planned subqueries not supported".into()))
        }
        NodeTag::T_FieldSelect => {
            let fselect = node.as_field_select().unwrap();
            let argtype = lsyscache::getBaseType(nodes_core::expr_type(fselect.arg))?;
            let reltype = lsyscache::get_typ_typrelid(argtype)?;
            // Column dep when the arg's rowtype has a pg_class entry; else the
            // result type itself (it may appear nowhere else).
            if reltype != InvalidOid {
                context.add(RELATION_RELATION_ID, reltype, fselect.fieldnum as i32);
            } else {
                context.add(TYPE_RELATION_ID, fselect.resulttype, 0);
            }
            if fselect.resultcollid != InvalidOid
                && fselect.resultcollid != DEFAULT_COLLATION_OID
            {
                context.add(CollationRelationId, fselect.resultcollid, 0);
            }
            walker(fselect.arg, context)
        }
        NodeTag::T_FieldStore => {
            let fstore = node.as_field_store().unwrap();
            let reltype = lsyscache::get_typ_typrelid(fstore.resulttype)?;
            if reltype != InvalidOid {
                for fieldnum in &fstore.fieldnums {
                    context.add(RELATION_RELATION_ID, reltype, fieldnum);
                }
            } else {
                context.add(TYPE_RELATION_ID, fstore.resulttype, 0);
            }
            walker(fstore.arg, context)?;
            walk_list(&fstore.newvals, context)
        }
        NodeTag::T_CoerceViaIO => {
            let iocoerce = node.as_coerce_via_io().unwrap();
            context.add(TYPE_RELATION_ID, iocoerce.resulttype, 0);
            if iocoerce.resultcollid != InvalidOid
                && iocoerce.resultcollid != DEFAULT_COLLATION_OID
            {
                context.add(CollationRelationId, iocoerce.resultcollid, 0);
            }
            walker(iocoerce.arg, context)
        }
        // C records collOid unconditionally: an explicit COLLATE always names
        // a real collation, default included.
        NodeTag::T_CollateExpr => {
            let coll = node.as_collate_expr().unwrap();
            context.add(CollationRelationId, coll.collOid, 0);
            walker(coll.arg, context)
        }
        NodeTag::T_RowExpr => {
            let rowexpr = node.as_row_expr().unwrap();
            context.add(TYPE_RELATION_ID, rowexpr.row_typeid, 0);
            walk_list(&rowexpr.args, context)
        }
        NodeTag::T_RowCompareExpr => {
            let rcexpr = node.as_row_compare_expr().unwrap();
            for opno in &rcexpr.opnos {
                context.add(OperatorRelationId, opno, 0);
            }
            for opfamily in &rcexpr.opfamilies {
                context.add(OperatorFamilyRelationId, opfamily, 0);
            }
            walk_list(&rcexpr.largs, context)?;
            walk_list(&rcexpr.rargs, context)
        }
        NodeTag::T_CoerceToDomain => {
            let cd = node.as_coerce_to_domain().unwrap();
            context.add(TYPE_RELATION_ID, cd.resulttype, 0);
            walker(cd.arg, context)
        }
        NodeTag::T_NextValueExpr => {
            let nve = node.as_variant::<types_nodes::primnodes::NextValueExpr>().unwrap();
            context.add(RELATION_RELATION_ID, nve.seqid, 0);
            Ok(())
        }
        NodeTag::T_OnConflictExpr => {
            let onconflict = node.as_on_conflict_expr().unwrap();
            if onconflict.constraint != InvalidOid {
                context.add(CONSTRAINT_RELATION_ID, onconflict.constraint, 0);
            }
            walk_list(&onconflict.arbiterElems, context)?;
            walk_opt(onconflict.arbiterWhere, context)?;
            walk_list(&onconflict.onConflictSet, context)?;
            walk_opt(onconflict.onConflictWhere, context)?;
            walk_list(&onconflict.exclRelTlist, context)
        }
        NodeTag::T_WindowClause => {
            let wc = node.as_window_clause().unwrap();
            if wc.startInRangeFunc != InvalidOid {
                context.add(types_core::PROCEDURE_RELATION_ID, wc.startInRangeFunc, 0);
            }
            if wc.endInRangeFunc != InvalidOid {
                context.add(types_core::PROCEDURE_RELATION_ID, wc.endInRangeFunc, 0);
            }
            if wc.inRangeColl != InvalidOid && wc.inRangeColl != DEFAULT_COLLATION_OID {
                context.add(CollationRelationId, wc.inRangeColl, 0);
            }
            walk_list(&wc.partitionClause, context)?;
            walk_list(&wc.orderClause, context)?;
            walk_opt(wc.startOffset, context)?;
            walk_opt(wc.endOffset, context)
        }
        // C records cycle_mark_collation whenever valid, no default skip.
        NodeTag::T_CTECycleClause => {
            let cc = node.as_cte_cycle_clause().unwrap();
            if cc.cycle_mark_type != InvalidOid {
                context.add(TYPE_RELATION_ID, cc.cycle_mark_type, 0);
            }
            if cc.cycle_mark_collation != InvalidOid {
                context.add(CollationRelationId, cc.cycle_mark_collation, 0);
            }
            if cc.cycle_mark_neop != InvalidOid {
                context.add(OperatorRelationId, cc.cycle_mark_neop, 0);
            }
            walk_opt(cc.cycle_mark_value, context)?;
            walk_opt(cc.cycle_mark_default, context)
        }
        NodeTag::T_CTESearchClause => Ok(()),
        NodeTag::T_SetOperationStmt => {
            let setop = node.as_set_operation_stmt().unwrap();
            walk_list(&setop.groupClauses, context)?;
            walk_opt(setop.larg, context)?;
            walk_opt(setop.rarg, context)
        }
        NodeTag::T_Aggref => {
            let aggref = node.as_aggref().unwrap();
            context.add(types_core::PROCEDURE_RELATION_ID, aggref.aggfnoid, 0);
            walk_list(&aggref.aggdirectargs, context)?;
            walk_list(&aggref.args, context)?;
            walk_list(&aggref.aggorder, context)?;
            walk_list(&aggref.aggdistinct, context)?;
            walk_opt(aggref.aggfilter, context)
        }
        NodeTag::T_RelabelType => {
            let relab = node.as_relabel_type().unwrap();
            context.add(TYPE_RELATION_ID, relab.resulttype, 0);
            if relab.resultcollid != InvalidOid
                && relab.resultcollid != DEFAULT_COLLATION_OID
            {
                context.add(CollationRelationId, relab.resultcollid, 0);
            }
            walker(relab.arg, context)
        }
        // C has no CaseTestExpr case: expression_tree_walker leaf, no deps.
        NodeTag::T_CaseTestExpr => Ok(()),
        NodeTag::T_ArrayCoerceExpr => {
            let ac = node.as_array_coerce_expr().unwrap();
            context.add(TYPE_RELATION_ID, ac.resulttype, 0);
            if ac.resultcollid != InvalidOid && ac.resultcollid != DEFAULT_COLLATION_OID {
                context.add(CollationRelationId, ac.resultcollid, 0);
            }
            walker(ac.arg, context)?;
            walk_opt(ac.elemexpr, context)
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let cvt = node.as_convert_rowtype_expr().unwrap();
            context.add(TYPE_RELATION_ID, cvt.resulttype, 0);
            walker(cvt.arg, context)
        }
        // C has no find_expr_references_walker case for SQLValueFunction: it
        // falls through to expression_tree_walker as a leaf (built-in pinned
        // result types; no dependency recorded).
        NodeTag::T_SQLValueFunction => Ok(()),
        NodeTag::T_BoolExpr => walk_list(&node.as_bool_expr().unwrap().args, context),
        // C has no case for CoalesceExpr: expression_tree_walker walks args.
        NodeTag::T_CoalesceExpr => walk_list(&node.as_coalesce_expr().unwrap().args, context),
        // C has no XmlExpr case: expression_tree_walker recursion only.
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            walk_list(&x.named_args, context)?;
            walk_list(&x.args, context)
        }
        NodeTag::T_TableFunc => {
            let tf = node.as_table_func().unwrap();
            for typid in &tf.coltypes {
                context.add(TYPE_RELATION_ID, typid, 0);
            }
            for collid in &tf.colcollations {
                if collid != InvalidOid && collid != DEFAULT_COLLATION_OID {
                    context.add(CollationRelationId, collid, 0);
                }
            }
            walk_list(&tf.ns_uris, context)?;
            walk_opt(tf.docexpr, context)?;
            walk_opt(tf.rowexpr, context)?;
            for e in tf.colexprs.iter() {
                walk_opt(e, context)?;
            }
            for e in tf.coldefexprs.iter() {
                walk_opt(e, context)?;
            }
            for e in tf.colvalexprs.iter() {
                walk_opt(e, context)?;
            }
            walk_list(&tf.passingvalexprs, context)
        }
        NodeTag::T_RangeTblFunction => {
            let rtfunc = node
                .as_variant::<types_nodes::parsenodes::RangeTblFunction>()
                .unwrap();
            for typid in &rtfunc.funccoltypes {
                context.add(TYPE_RELATION_ID, typid, 0);
            }
            for collid in &rtfunc.funccolcollations {
                if collid != InvalidOid && collid != DEFAULT_COLLATION_OID {
                    context.add(CollationRelationId, collid, 0);
                }
            }
            walk_opt(rtfunc.funcexpr, context)
        }
        NodeTag::T_TargetEntry => walker(node.as_target_entry().unwrap().expr, context),
        NodeTag::T_RangeTblRef => Ok(()),
        NodeTag::T_Param => {
            let param = node.as_variant::<types_nodes::primnodes::Param>().unwrap();
            context.add(TYPE_RELATION_ID, param.paramtype, 0);
            if param.paramcollid != InvalidOid && param.paramcollid != DEFAULT_COLLATION_OID {
                context.add(CollationRelationId, param.paramcollid, 0);
            }
            Ok(())
        }
        NodeTag::T_SubLink => {
            let sublink = node.as_sub_link().unwrap();
            walk_opt(sublink.testexpr, context)?;
            walker(sublink.subselect, context)
        }
        NodeTag::T_CommonTableExpr => {
            let cte = node.as_common_table_expr().unwrap();
            walk_opt(cte.ctequery, context)?;
            walk_opt(cte.search_clause, context)?;
            walk_opt(cte.cycle_clause, context)
        }
        NodeTag::T_SortGroupClause => {
            let sgc = node.as_sort_group_clause().unwrap();
            context.add(OperatorRelationId, sgc.eqop, 0);
            if sgc.sortop != InvalidOid {
                context.add(OperatorRelationId, sgc.sortop, 0);
            }
            Ok(())
        }
        NodeTag::T_FromExpr => {
            let from = node.as_from_expr().unwrap();
            walk_list(&from.fromlist, context)?;
            walk_opt(from.quals, context)
        }
        NodeTag::T_JoinExpr => {
            let join = node.as_join_expr().unwrap();
            walker(join.larg, context)?;
            walker(join.rarg, context)?;
            walk_opt(join.quals, context)
        }
        NodeTag::T_List => {
            let list = node.as_variant::<NodeList<'mcx>>().unwrap();
            walk_list(list, context)
        }
        NodeTag::T_Query => walk_query(node.as_query().unwrap(), context),
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                walker(e, context)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            walk_list(&c.args, context)?;
            for e in [c.func, c.coercion].into_iter().flatten() {
                walker(e, context)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => {
            match node.as_json_is_predicate().unwrap().expr {
                Some(e) => walker(e, context),
                None => Ok(()),
            }
        }
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => walker(e, context),
            None => Ok(()),
        },
        NodeTag::T_CoerceViaIO => {
            let io = node.as_coerce_via_io().unwrap();
            // No exposed function; depend on the type (and collation).
            context.add(TYPE_RELATION_ID, io.resulttype, 0);
            if io.resultcollid != InvalidOid && io.resultcollid != DEFAULT_COLLATION_OID {
                context.add(CollationRelationId, io.resultcollid, 0);
            }
            walker(io.arg, context)
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                walker(e, context)?;
            }
            walk_list(&j.passing_values, context)?;
            Ok(())
        }
        // No C case (dependency.c:2330): expression_tree_walker recursion only;
        // tags unknown to nodes_core stay loud there.
        _ => {
            struct Recurse<'a, 'w, 'mcx: 'w>(&'a mut FindExprContext<'w, 'mcx>);
            impl<'a, 'w, 'mcx: 'w> nodes_core::NodeWalker<'mcx> for Recurse<'a, 'w, 'mcx> {
                fn visit(&mut self, n: Node<'mcx>) -> PgResult<bool> {
                    walker(n, self.0)?;
                    Ok(false)
                }
            }
            nodes_core::expression_tree_walker(node, &mut Recurse(context))?;
            Ok(())
        }
    }
}

fn process_function_rte_ref<'w, 'mcx: 'w>(
    rte: &'mcx RangeTblEntry<'mcx>,
    attnum: i16,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    let mut atts_done: i32 = 0;
    for f in &rte.functions {
        let rtfunc = f.as_variant::<RangeTblFunction>().expect("functions holds RangeTblFunction");
        if attnum as i32 > atts_done && attnum as i32 <= atts_done + rtfunc.funccolcount {
            // A coldeflist means RECORD: no column dep possible. DIVERGENCE:
            // C probes get_expr_result_tupdesc(funcexpr, true) and tests
            // tdtypeid; the funcexpr's resolved result type (base type of
            // exprType) makes the same named-composite/RECORD/scalar split.
            if !rtfunc.funccolnames.is_nil() {
                return Ok(());
            }
            let Some(funcexpr) = rtfunc.funcexpr else {
                return Ok(());
            };
            let argtype = lsyscache::getBaseType(nodes_core::expr_type(funcexpr))?;
            if argtype == RECORDOID {
                return Ok(());
            }
            let reltype = lsyscache::get_typ_typrelid(argtype)?;
            if reltype != InvalidOid {
                context.add(RELATION_RELATION_ID, reltype, attnum as i32 - atts_done);
            }
            return Ok(());
        }
        atts_done += rtfunc.funccolcount;
    }

    if rte.funcordinality && attnum as i32 == atts_done + 1 {
        return Ok(());
    }

    Err(Box::new(
        PgError::error(format!(
            "column {} of relation \"{}\" does not exist",
            attnum,
            rte.eref
                .and_then(|e| e.aliasname)
                .expect("RTE_FUNCTION has an eref aliasname")
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
    ))
}

fn walk_query<'w, 'mcx: 'w>(
    query: &'mcx Query<'mcx>,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    for rte_node in &query.rtable {
        let rte = rte_node
            .as_range_tbl_entry()
            .expect("rtable entry is a RangeTblEntry");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                context.add(RELATION_RELATION_ID, rte.relid, 0);
            }
            RTEKind::RTE_JOIN => {
                // Only merged JOIN USING alias entries can carry type-coercion
                // functions; plain Vars there are covered by the join quals.
                context.rtables.insert(0, &query.rtable);
                for i in 0..rte.joinmergedcols as usize {
                    let aliasvar = rte.joinaliasvars.nth(i);
                    if aliasvar.as_var().is_none() {
                        walker(aliasvar, context)?;
                    }
                }
                context.rtables.remove(0);
            }
            // Tuplestores have no cataloged representation to depend on.
            RTEKind::RTE_NAMEDTUPLESTORE => {
                return Err(Box::new(
                    PgError::error(format!(
                        "transition table \"{}\" cannot be referenced in a persistent object",
                        rte.eref
                            .and_then(|e| e.aliasname)
                            .expect("RTE_NAMEDTUPLESTORE has an eref aliasname")
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            // RTE_CTE/RTE_VALUES collations only duplicate ones referenced
            // elsewhere in the Query (C dependency.c:2153); RTE_FUNCTION
            // collations ride with the RangeTblFunction recursion.
            RTEKind::RTE_SUBQUERY
            | RTEKind::RTE_CTE
            | RTEKind::RTE_TABLEFUNC
            | RTEKind::RTE_FUNCTION
            | RTEKind::RTE_VALUES
            // C's default arm; groupexprs ride the rtable recursion below.
            | RTEKind::RTE_GROUP => {}
            other => walker_unported(&format!("rtekind {other:?}")),
        }
    }

    if matches!(query.commandType, CmdType::CMD_INSERT | CmdType::CMD_UPDATE) {
        if query.resultRelation <= 0 || query.resultRelation as usize > query.rtable.len()
        {
            return Err(walker_error(format!(
                "invalid resultRelation {}",
                query.resultRelation
            )));
        }
        let rte = query
            .rtable
            .nth(query.resultRelation as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable entry is a RangeTblEntry");
        if rte.rtekind == RTEKind::RTE_RELATION {
            for tle_node in &query.targetList {
                let tle = tle_node.as_target_entry().expect("targetList holds TargetEntry");
                if tle.resjunk {
                    continue;
                }
                context.add(RELATION_RELATION_ID, rte.relid, tle.resno as i32);
            }
        }
    }

    for con_oid in &query.constraintDeps {
        context.add(CONSTRAINT_RELATION_ID, con_oid, 0);
    }

    // query_tree_walker(QTW_IGNORE_JOINALIASES | QTW_EXAMINE_SORTGROUP).
    context.rtables.insert(0, &query.rtable);
    let result = walk_query_fields(query, context);
    context.rtables.remove(0);
    result
}

fn walk_query_fields<'w, 'mcx: 'w>(
    query: &'mcx Query<'mcx>,
    context: &mut FindExprContext<'w, 'mcx>,
) -> PgResult<()> {
    walk_list(&query.targetList, context)?;
    walk_list(&query.withCheckOptions, context)?;
    walk_opt(query.onConflict, context)?;
    walk_list(&query.mergeActionList, context)?;
    walk_opt(query.mergeJoinCondition, context)?;
    walk_list(&query.returningList, context)?;
    if let Some(jointree) = query.jointree {
        walk_list(&jointree.fromlist, context)?;
        walk_opt(jointree.quals, context)?;
    }
    walk_opt(query.setOperations, context)?;
    walk_opt(query.havingQual, context)?;
    walk_opt(query.limitOffset, context)?;
    walk_opt(query.limitCount, context)?;
    walk_list(&query.groupClause, context)?;
    walk_list(&query.windowClause, context)?;
    walk_list(&query.sortClause, context)?;
    walk_list(&query.distinctClause, context)?;
    walk_list(&query.cteList, context)?;
    for rte_node in &query.rtable {
        let rte = rte_node
            .as_range_tbl_entry()
            .expect("rtable entry is a RangeTblEntry");
        match rte.rtekind {
            RTEKind::RTE_RELATION => walk_opt(rte.tablesample, context)?,
            RTEKind::RTE_SUBQUERY => {
                walk_query(rte.subquery.expect("RTE_SUBQUERY has a subquery"), context)?
            }
            RTEKind::RTE_JOIN | RTEKind::RTE_CTE => {}
            RTEKind::RTE_TABLEFUNC => {
                walk_opt(rte.tablefunc, context)?
            }
            RTEKind::RTE_FUNCTION => walk_list(&rte.functions, context)?,
            RTEKind::RTE_VALUES => walk_list(&rte.values_lists, context)?,
            // range_table_walker without QTW_IGNORE_GROUPEXPRS: the grouping
            // expressions live only here (the tlist holds GROUP Vars).
            RTEKind::RTE_GROUP => walk_list(&rte.groupexprs, context)?,
            other => walker_unported(&format!("rtekind {other:?}")),
        }
        walk_list(&rte.securityQuals, context)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datum::Datum;
    use mcx::MemoryContext;
    use types_nodes::jointype::JoinType;
    use types_nodes::parsenodes::RangeTblEntry;
    use types_nodes::primnodes::JoinExpr;

    const T1: Oid = 50001;
    const T2: Oid = 50002;
    const INT4OID: Oid = 23;
    const INT8OID: Oid = 20;
    const TEXTOID: Oid = 25;
    const INT4EQ_OP: Oid = 96;
    const INT84GT_OP: Oid = 419;

    fn rel_rte<'m>(mcx: Mcx<'m>, relid: Oid) -> Node<'m> {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: b'r',
                ..Default::default()
            },
        )
        .unwrap()
    }

    // SELECT t1.a, t1.b, t2.d FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b > 10
    // (t1: a int4, b int8, c text; t2: a int4, d text).
    fn join_select_query(mcx: Mcx<'_>) -> Node<'_> {
        let join_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_JOIN,
                jointype: JoinType::JOIN_INNER,
                ..Default::default()
            },
        )
        .unwrap();
        let rtable =
            NodeList::from_slice(mcx, &[rel_rte(mcx, T1), rel_rte(mcx, T2), join_rte])
                .unwrap();

        let t1_a = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let t1_b = Node::mk_var(mcx, 1, 2, INT8OID, -1, 0, 0).unwrap();
        let t2_a = Node::mk_var(mcx, 2, 1, INT4OID, -1, 0, 0).unwrap();
        let t2_d = Node::mk_var(mcx, 2, 2, TEXTOID, -1, 0, 0).unwrap();

        let target_list = NodeList::from_slice(
            mcx,
            &[
                Node::mk_target_entry(mcx, t1_a, 1, Some("a"), false).unwrap(),
                Node::mk_target_entry(mcx, t1_b, 2, Some("b"), false).unwrap(),
                Node::mk_target_entry(mcx, t2_d, 3, Some("d"), false).unwrap(),
            ],
        )
        .unwrap();

        let join_qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT4EQ_OP,
                opfuncid: 65,
                opresulttype: 16,
                args: NodeList::from_slice(mcx, &[t1_a, t2_a]).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let join_expr = Node::mk(
            mcx,
            JoinExpr {
                jointype: JoinType::JOIN_INNER,
                isNatural: false,
                larg: Node::mk_range_tbl_ref(mcx, 1).unwrap(),
                rarg: Node::mk_range_tbl_ref(mcx, 2).unwrap(),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(join_qual),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();

        let ten = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(10), false, true)
            .unwrap();
        let where_qual = Node::mk(
            mcx,
            types_nodes::primnodes::OpExpr {
                opno: INT84GT_OP,
                opfuncid: 722,
                opresulttype: 16,
                args: NodeList::from_slice(mcx, &[t1_b, ten]).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();

        let from_expr = Node::mk_from_expr(
            mcx,
            NodeList::make1(mcx, join_expr).unwrap(),
            Some(where_qual),
        )
        .unwrap();

        Node::mk(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable,
                targetList: target_list,
                jointree: Some(from_expr.as_from_expr().unwrap()),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn join_select_yields_column_refs_only() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let query = join_select_query(mcx);
        let refs = find_expr_references(query, &NodeList::nil()).unwrap();

        // The rtable whole-rel refs (objsubid 0) must be absorbed into the
        // column refs by eliminate_duplicate_dependencies.
        let mut expected = vec![
            ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 1),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 2),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T2, 1),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T2, 2),
            ObjectAddress::set(OperatorRelationId, INT4EQ_OP),
            ObjectAddress::set(OperatorRelationId, INT84GT_OP),
            ObjectAddress::set(TYPE_RELATION_ID, INT4OID),
        ];
        expected.sort_by(object_address_comparator);
        assert_eq!(refs, expected);
        assert!(!refs
            .iter()
            .any(|r| r.classId == RELATION_RELATION_ID && r.objectSubId == 0));
    }

    #[test]
    fn dedup_absorbs_whole_object_into_column_ref() {
        let mut addrs = vec![
            ObjectAddress::set(RELATION_RELATION_ID, T1),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 2),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 2),
            ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 1),
        ];
        eliminate_duplicate_dependencies(&mut addrs);
        assert_eq!(
            addrs,
            vec![
                ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 1),
                ObjectAddress::sub_set(RELATION_RELATION_ID, T1, 2),
            ]
        );
    }
}
