//! outfuncs.c nodeToString for the node sets stored in pg_attrdef.adbin /
//! pg_constraint.conbin (DEFAULT/CHECK corpus), pg_trigger.tgqual, and
//! pg_rewrite.ev_action (view SELECT-rule Query trees). Every other node tag
//! is a loud panic naming the C writer. Output is byte-compatible with C 18.3
//! nodeToString (write_location_fields=false: every location renders as -1).

#![allow(non_snake_case)]

use core::fmt::Write;

use datum::Datum;
use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList, OptNodeList};
use types_nodes::parsenodes::{
    CommonTableExpr, Query, RTEKind, RTEPermissionInfo, RangeTblEntry, SetOperationStmt,
    SortGroupClause, TableSampleClause,
};
use types_nodes::primnodes::{
    Aggref, Alias, ArrayCoerceExpr, BoolExpr, BoolExprType, CoerceToDomain, CoerceToDomainValue,
    CoerceViaIO, Const, ConvertRowtypeExpr, FromExpr, FuncExpr, JoinExpr, NamedArgExpr, NullTest,
    OpExpr, PlaceHolderVar, RangeTblRef, RelabelType, ScalarArrayOpExpr, SubLink, TableFunc, TargetEntry, Var,
    XmlExpr,
};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum};
use types_nodes::{Boolean, Float, Integer, Node, NodeTag};

pub fn nodeToString<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    out_node(&mut out, node)?;
    Ok(out)
}

// Query reachable only as RangeTblEntry.subquery's &Query (no node handle).
pub fn queryToString<'mcx>(mcx: Mcx<'mcx>, q: &Query<'_>) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    out_query(&mut out, q)?;
    Ok(out)
}

macro_rules! w {
    ($out:expr, $($arg:tt)*) => {
        write!($out, $($arg)*).expect("outfuncs append")
    };
}

fn out_node(out: &mut PgString<'_>, node: Node<'_>) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => out_var(out, node.as_variant::<Var>().expect("Var")),
        NodeTag::T_Const => out_const(out, node.as_variant::<Const>().expect("Const")),
        NodeTag::T_OpExpr => out_op_expr(out, node.as_variant::<OpExpr>().expect("OpExpr"))?,
        NodeTag::T_DistinctExpr => out_distinct_expr(
            out,
            node.as_variant::<types_nodes::primnodes::DistinctExpr>().expect("DistinctExpr"),
        )?,
        NodeTag::T_NullIfExpr => out_null_if_expr(
            out,
            node.as_variant::<types_nodes::primnodes::NullIfExpr>().expect("NullIfExpr"),
        )?,
        NodeTag::T_FuncExpr => {
            out_func_expr(out, node.as_variant::<FuncExpr>().expect("FuncExpr"))?
        }
        NodeTag::T_NamedArgExpr => {
            out_named_arg_expr(out, node.as_variant::<NamedArgExpr>().expect("NamedArgExpr"))?
        }
        NodeTag::T_BoolExpr => {
            out_bool_expr(out, node.as_variant::<BoolExpr>().expect("BoolExpr"))?
        }
        NodeTag::T_NullTest => {
            out_null_test(out, node.as_variant::<NullTest>().expect("NullTest"))?
        }
        NodeTag::T_XmlExpr => {
            out_xml_expr(out, node.as_variant::<XmlExpr>().expect("XmlExpr"))?
        }
        NodeTag::T_ReturningExpr => {
            let r = node.as_returning_expr().expect("ReturningExpr");
            w!(out, "{{RETURNINGEXPR :retlevelsup {} :retold ", r.retlevelsup);
            out_bool(out, r.retold);
            w!(out, " :retexpr ");
            out_node(out, r.retexpr)?;
            w!(out, "}}");
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().expect("FieldSelect");
            w!(out, "{{FIELDSELECT :arg ");
            out_node(out, f.arg)?;
            w!(
                out,
                " :fieldnum {} :resulttype {} :resulttypmod {} :resultcollid {}}}",
                f.fieldnum, f.resulttype, f.resulttypmod, f.resultcollid
            );
        }
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().expect("FieldStore");
            w!(out, "{{FIELDSTORE :arg ");
            out_node(out, f.arg)?;
            w!(out, " :newvals ");
            out_list(out, &f.newvals)?;
            w!(out, " :fieldnums ");
            out_int_list(out, &f.fieldnums);
            w!(out, " :resulttype {}}}", f.resulttype);
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().expect("ArrayExpr");
            w!(
                out,
                "{{ARRAYEXPR :array_typeid {} :array_collid {} :element_typeid {} :elements ",
                a.array_typeid, a.array_collid, a.element_typeid
            );
            out_list(out, &a.elements)?;
            w!(out, " :multidims ");
            out_bool(out, a.multidims);
            w!(out, " :list_start -1 :list_end -1 :location -1}}");
        }
        NodeTag::T_CaseTestExpr => {
            let c = node.as_case_test_expr().expect("CaseTestExpr");
            w!(
                out,
                "{{CASETESTEXPR :typeId {} :typeMod {} :collation {}}}",
                c.typeId, c.typeMod, c.collation
            );
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().expect("CaseExpr");
            w!(out, "{{CASEEXPR :casetype {} :casecollid {} :arg ", c.casetype, c.casecollid);
            out_opt_node(out, c.arg)?;
            w!(out, " :args ");
            out_list(out, &c.args)?;
            w!(out, " :defresult ");
            out_opt_node(out, c.defresult)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_CaseWhen => {
            let c = node.as_case_when().expect("CaseWhen");
            w!(out, "{{CASEWHEN :expr ");
            out_opt_node(out, c.expr)?;
            w!(out, " :result ");
            out_opt_node(out, c.result)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_MergeSupportFunc => {
            let m = node.as_merge_support_func().expect("MergeSupportFunc");
            w!(
                out,
                "{{MERGESUPPORTFUNC :msftype {} :msfcollid {} :location -1}}",
                m.msftype, m.msfcollid
            );
        }
        NodeTag::T_WindowFunc => {
            let f = node.as_window_func().expect("WindowFunc");
            w!(
                out,
                "{{WINDOWFUNC :winfnoid {} :wintype {} :wincollid {} :inputcollid {} :args ",
                f.winfnoid, f.wintype, f.wincollid, f.inputcollid
            );
            out_list(out, &f.args)?;
            w!(out, " :aggfilter ");
            out_opt_node(out, f.aggfilter)?;
            w!(out, " :runCondition ");
            out_list(out, &f.runCondition)?;
            w!(out, " :winref {} :winstar ", f.winref);
            out_bool(out, f.winstar);
            w!(out, " :winagg ");
            out_bool(out, f.winagg);
            w!(out, " :location -1}}");
        }
        NodeTag::T_WindowClause => {
            let c = node.as_window_clause().expect("WindowClause");
            w!(out, "{{WINDOWCLAUSE :name ");
            out_str(out, c.name);
            w!(out, " :refname ");
            out_str(out, c.refname);
            w!(out, " :partitionClause ");
            out_list(out, &c.partitionClause)?;
            w!(out, " :orderClause ");
            out_list(out, &c.orderClause)?;
            w!(out, " :frameOptions {} :startOffset ", c.frameOptions);
            out_opt_node(out, c.startOffset)?;
            w!(out, " :endOffset ");
            out_opt_node(out, c.endOffset)?;
            w!(
                out,
                " :startInRangeFunc {} :endInRangeFunc {} :inRangeColl {} :inRangeAsc ",
                c.startInRangeFunc, c.endInRangeFunc, c.inRangeColl
            );
            out_bool(out, c.inRangeAsc);
            w!(out, " :inRangeNullsFirst ");
            out_bool(out, c.inRangeNullsFirst);
            w!(out, " :winref {} :copiedOrder ", c.winref);
            out_bool(out, c.copiedOrder);
            w!(out, "}}");
        }
        NodeTag::T_SetOperationStmt => {
            let s = node.as_set_operation_stmt().expect("SetOperationStmt");
            w!(out, "{{SETOPERATIONSTMT :op {} :all ", s.op as u32);
            out_bool(out, s.all);
            w!(out, " :larg ");
            out_opt_node(out, s.larg)?;
            w!(out, " :rarg ");
            out_opt_node(out, s.rarg)?;
            w!(out, " :colTypes ");
            out_oid_list(out, &s.colTypes);
            w!(out, " :colTypmods ");
            out_int_list(out, &s.colTypmods);
            w!(out, " :colCollations ");
            out_oid_list(out, &s.colCollations);
            w!(out, " :groupClauses ");
            out_list(out, &s.groupClauses)?;
            w!(out, "}}");
        }
        NodeTag::T_RangeTblFunction => out_range_tbl_function(
            out,
            node.as_variant::<types_nodes::parsenodes::RangeTblFunction>()
                .expect("RangeTblFunction"),
        )?,
        NodeTag::T_TableFunc => {
            out_table_func(out, node.as_variant::<TableFunc>().expect("TableFunc"))?
        }
        NodeTag::T_RelabelType => {
            out_relabel_type(out, node.as_variant::<RelabelType>().expect("RelabelType"))?
        }
        NodeTag::T_PlaceHolderVar => out_place_holder_var(
            out,
            node.as_variant::<PlaceHolderVar>().expect("PlaceHolderVar"),
        )?,
        NodeTag::T_CoalesceExpr => {
            let c = node.as_coalesce_expr().expect("CoalesceExpr");
            w!(
                out,
                "{{COALESCEEXPR :coalescetype {} :coalescecollid {} :args ",
                c.coalescetype, c.coalescecollid
            );
            out_list(out, &c.args)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_List => out_list(out, node.as_list().expect("List"))?,
        NodeTag::T_CoerceViaIO => {
            out_coerce_via_io(out, node.as_variant::<CoerceViaIO>().expect("CoerceViaIO"))?
        }
        NodeTag::T_ArrayCoerceExpr => out_array_coerce_expr(
            out,
            node.as_variant::<ArrayCoerceExpr>().expect("ArrayCoerceExpr"),
        )?,
        NodeTag::T_ConvertRowtypeExpr => out_convert_rowtype_expr(
            out,
            node.as_variant::<ConvertRowtypeExpr>().expect("ConvertRowtypeExpr"),
        )?,
        NodeTag::T_CoerceToDomain => out_coerce_to_domain(
            out,
            node.as_variant::<CoerceToDomain>().expect("CoerceToDomain"),
        )?,
        NodeTag::T_CoerceToDomainValue => {
            let v = node.as_variant::<CoerceToDomainValue>().expect("CoerceToDomainValue");
            w!(
                out,
                "{{COERCETODOMAINVALUE :typeId {} :typeMod {} :collation {} :location -1}}",
                v.typeId, v.typeMod, v.collation
            );
        }
        NodeTag::T_SQLValueFunction => {
            let v = node
                .as_variant::<types_nodes::primnodes::SQLValueFunction>()
                .expect("SQLValueFunction");
            w!(
                out,
                "{{SQLVALUEFUNCTION :op {} :type {} :typmod {} :location -1}}",
                v.op as u32, v.r#type, v.typmod
            );
        }
        NodeTag::T_ScalarArrayOpExpr => out_scalar_array_op_expr(
            out,
            node.as_variant::<ScalarArrayOpExpr>().expect("ScalarArrayOpExpr"),
        )?,
        NodeTag::T_PartitionBoundSpec => out_partition_bound_spec(
            out,
            node.as_variant::<PartitionBoundSpec>().expect("PartitionBoundSpec"),
        )?,
        NodeTag::T_PartitionRangeDatum => out_partition_range_datum(
            out,
            node.as_variant::<PartitionRangeDatum>().expect("PartitionRangeDatum"),
        )?,
        NodeTag::T_BooleanTest => {
            let bt = node
                .as_variant::<types_nodes::primnodes::BooleanTest>()
                .expect("BooleanTest");
            w!(out, "{{BOOLEANTEST :arg ");
            out_opt_node(out, bt.arg)?;
            w!(out, " :booltesttype {} :location -1}}", bt.booltesttype as u32);
        }
        NodeTag::T_SetToDefault => {
            let d = node
                .as_variant::<types_nodes::primnodes::SetToDefault>()
                .expect("SetToDefault");
            w!(
                out,
                "{{SETTODEFAULT :typeId {} :typeMod {} :collation {} :location -1}}",
                d.typeId, d.typeMod, d.collation
            );
        }
        NodeTag::T_JsonFormat => {
            out_json_format(out, node.as_json_format().expect("JsonFormat"))
        }
        NodeTag::T_JsonReturning => {
            out_json_returning(out, node.as_json_returning().expect("JsonReturning"))
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().expect("JsonValueExpr");
            w!(out, "{{JSONVALUEEXPR :raw_expr ");
            out_opt_node(out, j.raw_expr)?;
            w!(out, " :formatted_expr ");
            out_opt_node(out, j.formatted_expr)?;
            w!(out, " :format ");
            out_opt_json_format(out, j.format);
            w!(out, "}}");
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().expect("JsonConstructorExpr");
            w!(out, "{{JSONCONSTRUCTOREXPR :type {} :args ", c.r#type as u32);
            out_list(out, &c.args)?;
            w!(out, " :func ");
            out_opt_node(out, c.func)?;
            w!(out, " :coercion ");
            out_opt_node(out, c.coercion)?;
            w!(out, " :returning ");
            out_opt_json_returning(out, c.returning);
            w!(out, " :absent_on_null ");
            out_bool(out, c.absent_on_null);
            w!(out, " :unique ");
            out_bool(out, c.unique);
            w!(out, " :location -1}}");
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().expect("JsonIsPredicate");
            w!(out, "{{JSONISPREDICATE :expr ");
            out_opt_node(out, p.expr)?;
            w!(out, " :format ");
            out_opt_json_format(out, p.format);
            w!(out, " :item_type {} :unique_keys ", p.item_type as u32);
            out_bool(out, p.unique_keys);
            w!(out, " :location -1}}");
        }
        NodeTag::T_JsonBehavior => {
            let b = node.as_json_behavior().expect("JsonBehavior");
            w!(out, "{{JSONBEHAVIOR :btype {} :expr ", b.btype as u32);
            out_opt_node(out, b.expr)?;
            w!(out, " :coerce ");
            out_bool(out, b.coerce);
            w!(out, " :location -1}}");
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().expect("JsonExpr");
            w!(out, "{{JSONEXPR :op {} :column_name ", j.op as u32);
            out_str(out, j.column_name);
            w!(out, " :formatted_expr ");
            out_opt_node(out, j.formatted_expr)?;
            w!(out, " :format ");
            out_opt_json_format(out, j.format);
            w!(out, " :path_spec ");
            out_opt_node(out, j.path_spec)?;
            w!(out, " :returning ");
            out_opt_json_returning(out, j.returning);
            w!(out, " :passing_names ");
            out_list(out, &j.passing_names)?;
            w!(out, " :passing_values ");
            out_list(out, &j.passing_values)?;
            w!(out, " :on_empty ");
            out_opt_node(out, j.on_empty)?;
            w!(out, " :on_error ");
            out_opt_node(out, j.on_error)?;
            w!(out, " :use_io_coercion ");
            out_bool(out, j.use_io_coercion);
            w!(out, " :use_json_coercion ");
            out_bool(out, j.use_json_coercion);
            w!(
                out,
                " :wrapper {} :omit_quotes ",
                j.wrapper as u32
            );
            out_bool(out, j.omit_quotes);
            w!(out, " :collation {} :location -1}}", j.collation);
        }
        NodeTag::T_JsonTablePath => {
            let p = node.as_json_table_path().expect("JsonTablePath");
            w!(out, "{{JSONTABLEPATH :value ");
            out_opt_node(out, p.value)?;
            w!(out, " :name ");
            out_str(out, p.name);
            w!(out, "}}");
        }
        NodeTag::T_JsonTablePathScan => {
            let s = node.as_json_table_path_scan().expect("JsonTablePathScan");
            w!(out, "{{JSONTABLEPATHSCAN :path ");
            out_opt_node(out, s.path)?;
            w!(out, " :errorOnError ");
            out_bool(out, s.errorOnError);
            w!(out, " :child ");
            out_opt_node(out, s.child)?;
            w!(out, " :colMin {} :colMax {}}}", s.colMin, s.colMax);
        }
        NodeTag::T_JsonTableSiblingJoin => {
            let j = node.as_json_table_sibling_join().expect("JsonTableSiblingJoin");
            w!(out, "{{JSONTABLESIBLINGJOIN :lplan ");
            out_opt_node(out, j.lplan)?;
            w!(out, " :rplan ");
            out_opt_node(out, j.rplan)?;
            w!(out, "}}");
        }
        NodeTag::T_Query => out_query(out, node.as_variant::<Query>().expect("Query"))?,
        NodeTag::T_RangeTblEntry => {
            out_range_tbl_entry(out, node.as_variant::<RangeTblEntry>().expect("RangeTblEntry"))?
        }
        NodeTag::T_TableSampleClause => {
            let t = node.as_variant::<TableSampleClause>().expect("TableSampleClause");
            w!(out, "{{TABLESAMPLECLAUSE :tsmhandler {} :args ", t.tsmhandler);
            out_list(out, &t.args)?;
            w!(out, " :repeatable ");
            out_opt_node(out, t.repeatable)?;
            w!(out, "}}");
        }
        NodeTag::T_RTEPermissionInfo => out_rte_permission_info(
            out,
            node.as_variant::<RTEPermissionInfo>().expect("RTEPermissionInfo"),
        ),
        NodeTag::T_Alias => out_alias(out, node.as_variant::<Alias>().expect("Alias"))?,
        NodeTag::T_FromExpr => {
            out_from_expr(out, node.as_variant::<FromExpr>().expect("FromExpr"))?
        }
        NodeTag::T_JoinExpr => {
            out_join_expr(out, node.as_variant::<JoinExpr>().expect("JoinExpr"))?
        }
        NodeTag::T_RangeTblRef => {
            out_range_tbl_ref(out, node.as_variant::<RangeTblRef>().expect("RangeTblRef"))
        }
        NodeTag::T_TargetEntry => {
            out_target_entry(out, node.as_variant::<TargetEntry>().expect("TargetEntry"))?
        }
        NodeTag::T_SortGroupClause => out_sort_group_clause(
            out,
            node.as_variant::<SortGroupClause>().expect("SortGroupClause"),
        ),
        NodeTag::T_GroupingSet => out_grouping_set(
            out,
            node.as_variant::<types_nodes::parsenodes::GroupingSet>().expect("GroupingSet"),
        )?,
        NodeTag::T_Aggref => out_aggref(out, node.as_variant::<Aggref>().expect("Aggref"))?,
        NodeTag::T_GroupingFunc => {
            let g = node
                .as_variant::<types_nodes::primnodes::GroupingFunc>()
                .expect("GroupingFunc");
            w!(out, "{{GROUPINGFUNC :args ");
            out_list(out, &g.args)?;
            w!(out, " :refs ");
            out_int_list(out, &g.refs);
            w!(out, " :cols ");
            out_int_list(out, &g.cols);
            w!(out, " :agglevelsup {} :location -1}}", g.agglevelsup);
        }
        NodeTag::T_SubLink => out_sub_link(out, node.as_variant::<SubLink>().expect("SubLink"))?,
        NodeTag::T_Param => {
            let p = node.as_variant::<types_nodes::primnodes::Param>().expect("Param");
            w!(
                out,
                "{{PARAM :paramkind {} :paramid {} :paramtype {} :paramtypmod {} \
                 :paramcollid {} :location -1}}",
                p.paramkind as u32, p.paramid, p.paramtype, p.paramtypmod, p.paramcollid
            );
        }
        NodeTag::T_CTESearchClause => out_cte_search_clause(
            out,
            node.as_variant::<types_nodes::parsenodes::CTESearchClause>()
                .expect("CTESearchClause"),
        )?,
        NodeTag::T_CTECycleClause => out_cte_cycle_clause(
            out,
            node.as_variant::<types_nodes::parsenodes::CTECycleClause>()
                .expect("CTECycleClause"),
        )?,
        NodeTag::T_NotifyStmt => out_notify_stmt(
            out,
            node.as_variant::<types_nodes::parsenodes::NotifyStmt>()
                .expect("NotifyStmt"),
        )?,
        NodeTag::T_NextValueExpr => out_next_value_expr(
            out,
            node.as_variant::<types_nodes::primnodes::NextValueExpr>()
                .expect("NextValueExpr"),
        )?,
        NodeTag::T_CommonTableExpr => out_common_table_expr(
            out,
            node.as_variant::<CommonTableExpr>().expect("CommonTableExpr"),
        )?,
        NodeTag::T_IntList => out_int_list(out, node.as_int_list().expect("IntList")),
        NodeTag::T_OidList => out_oid_list(out, node.as_oid_list().expect("OidList")),
        NodeTag::T_String => out_string_node(out, node.as_string().expect("String").sval),
        NodeTag::T_Integer => {
            w!(out, "{}", node.as_variant::<Integer>().expect("Integer").ival)
        }
        NodeTag::T_Float => w!(out, "{}", node.as_variant::<Float>().expect("Float").fval),
        NodeTag::T_Boolean => {
            out_bool(out, node.as_variant::<Boolean>().expect("Boolean").boolval)
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node
                .as_variant::<types_nodes::primnodes::SubscriptingRef>()
                .expect("SubscriptingRef");
            w!(
                out,
                "{{SUBSCRIPTINGREF :refcontainertype {} :refelemtype {} :refrestype {} \
                 :reftypmod {} :refcollid {} :refupperindexpr ",
                sr.refcontainertype, sr.refelemtype, sr.refrestype, sr.reftypmod, sr.refcollid
            );
            out_opt_list(out, &sr.refupperindexpr)?;
            w!(out, " :reflowerindexpr ");
            out_opt_list(out, &sr.reflowerindexpr)?;
            w!(out, " :refexpr ");
            out_opt_node(out, sr.refexpr)?;
            w!(out, " :refassgnexpr ");
            out_opt_node(out, sr.refassgnexpr)?;
            w!(out, "}}");
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_variant::<types_nodes::primnodes::CaseExpr>().expect("CaseExpr");
            w!(out, "{{CASEEXPR :casetype {} :casecollid {} :arg ", c.casetype, c.casecollid);
            out_opt_node(out, c.arg)?;
            w!(out, " :args ");
            out_list(out, &c.args)?;
            w!(out, " :defresult ");
            out_opt_node(out, c.defresult)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_variant::<types_nodes::primnodes::CaseWhen>().expect("CaseWhen");
            w!(out, "{{CASEWHEN :expr ");
            out_opt_node(out, cw.expr)?;
            w!(out, " :result ");
            out_opt_node(out, cw.result)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_CollateExpr => {
            let c = node
                .as_variant::<types_nodes::primnodes::CollateExpr>()
                .expect("CollateExpr");
            w!(out, "{{COLLATEEXPR :arg ");
            out_node(out, c.arg)?;
            w!(out, " :collOid {} :location -1}}", c.collOid);
        }
        NodeTag::T_RowExpr => {
            let r = node.as_variant::<types_nodes::primnodes::RowExpr>().expect("RowExpr");
            w!(out, "{{ROWEXPR :args ");
            out_list(out, &r.args)?;
            w!(
                out,
                " :row_typeid {} :row_format {} :colnames ",
                r.row_typeid, r.row_format as u32
            );
            out_list(out, &r.colnames)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_RowCompareExpr => {
            let r = node
                .as_variant::<types_nodes::primnodes::RowCompareExpr>()
                .expect("RowCompareExpr");
            w!(out, "{{ROWCOMPAREEXPR :cmptype {} :opnos ", r.cmptype);
            out_oid_list(out, &r.opnos);
            w!(out, " :opfamilies ");
            out_oid_list(out, &r.opfamilies);
            w!(out, " :inputcollids ");
            out_oid_list(out, &r.inputcollids);
            w!(out, " :largs ");
            out_list(out, &r.largs)?;
            w!(out, " :rargs ");
            out_list(out, &r.rargs)?;
            w!(out, "}}");
        }
        NodeTag::T_MinMaxExpr => {
            let m =
                node.as_variant::<types_nodes::primnodes::MinMaxExpr>().expect("MinMaxExpr");
            w!(
                out,
                "{{MINMAXEXPR :minmaxtype {} :minmaxcollid {} :inputcollid {} :op {} :args ",
                m.minmaxtype, m.minmaxcollid, m.inputcollid, m.op as u32
            );
            out_list(out, &m.args)?;
            w!(out, " :location -1}}");
        }
        NodeTag::T_WindowFunc => {
            let f =
                node.as_variant::<types_nodes::primnodes::WindowFunc>().expect("WindowFunc");
            w!(
                out,
                "{{WINDOWFUNC :winfnoid {} :wintype {} :wincollid {} :inputcollid {} :args ",
                f.winfnoid, f.wintype, f.wincollid, f.inputcollid
            );
            out_list(out, &f.args)?;
            w!(out, " :aggfilter ");
            out_opt_node(out, f.aggfilter)?;
            w!(out, " :runCondition ");
            out_list(out, &f.runCondition)?;
            w!(out, " :winref {} :winstar ", f.winref);
            out_bool(out, f.winstar);
            w!(out, " :winagg ");
            out_bool(out, f.winagg);
            w!(out, " :location -1}}");
        }
        NodeTag::T_SetOperationStmt => {
            let s = node.as_variant::<SetOperationStmt>().expect("SetOperationStmt");
            w!(out, "{{SETOPERATIONSTMT :op {} :all ", s.op as u32);
            out_bool(out, s.all);
            w!(out, " :larg ");
            out_opt_node(out, s.larg)?;
            w!(out, " :rarg ");
            out_opt_node(out, s.rarg)?;
            w!(out, " :colTypes ");
            out_oid_list(out, &s.colTypes);
            w!(out, " :colTypmods ");
            out_int_list(out, &s.colTypmods);
            w!(out, " :colCollations ");
            out_oid_list(out, &s.colCollations);
            w!(out, " :groupClauses ");
            out_list(out, &s.groupClauses)?;
            w!(out, "}}");
        }
        NodeTag::T_CurrentOfExpr => {
            let c = node
                .as_variant::<types_nodes::primnodes::CurrentOfExpr>()
                .expect("CurrentOfExpr");
            w!(out, "{{CURRENTOFEXPR :cvarno {} :cursor_name ", c.cvarno);
            out_str(out, c.cursor_name);
            w!(out, " :cursor_param {}}}", c.cursor_param);
        }
        NodeTag::T_RowMarkClause => {
            let r = node
                .as_variant::<types_nodes::parsenodes::RowMarkClause>()
                .expect("RowMarkClause");
            w!(
                out,
                "{{ROWMARKCLAUSE :rti {} :strength {} :waitPolicy {} :pushedDown ",
                r.rti, r.strength as u32, r.waitPolicy as u32
            );
            out_bool(out, r.pushedDown);
            w!(out, "}}");
        }
        NodeTag::T_MergeAction => {
            let m = node
                .as_variant::<types_nodes::primnodes::MergeAction>()
                .expect("MergeAction");
            w!(
                out,
                "{{MERGEACTION :matchKind {} :commandType {} :override {}",
                m.matchKind as u32,
                m.commandType as u32,
                m.r#override as u32
            );
            w!(out, " :qual ");
            out_opt_node(out, m.qual)?;
            w!(out, " :targetList ");
            out_list(out, &m.targetList)?;
            w!(out, " :updateColnos ");
            out_int_list(out, &m.updateColnos);
            w!(out, "}}");
        }
        NodeTag::T_OnConflictExpr => {
            let c = node
                .as_variant::<types_nodes::primnodes::OnConflictExpr>()
                .expect("OnConflictExpr");
            w!(out, "{{ONCONFLICTEXPR :action {}", c.action as u32);
            w!(out, " :arbiterElems ");
            out_list(out, &c.arbiterElems)?;
            w!(out, " :arbiterWhere ");
            out_opt_node(out, c.arbiterWhere)?;
            w!(out, " :constraint {}", c.constraint);
            w!(out, " :onConflictSet ");
            out_list(out, &c.onConflictSet)?;
            w!(out, " :onConflictWhere ");
            out_opt_node(out, c.onConflictWhere)?;
            w!(out, " :exclRelIndex {}", c.exclRelIndex);
            w!(out, " :exclRelTlist ");
            out_list(out, &c.exclRelTlist)?;
            w!(out, "}}");
        }
        NodeTag::T_InferenceElem => {
            let ie = node
                .as_variant::<types_nodes::primnodes::InferenceElem>()
                .expect("InferenceElem");
            w!(out, "{{INFERENCEELEM :expr ");
            out_opt_node(out, ie.expr)?;
            w!(out, " :infercollid {} :inferopclass {}}}", ie.infercollid, ie.inferopclass);
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_variant::<types_nodes::primnodes::SubPlan>().expect("SubPlan");
            w!(out, "{{SUBPLAN :subLinkType {}", sp.subLinkType as u32);
            w!(out, " :testexpr ");
            out_opt_node(out, sp.testexpr)?;
            w!(out, " :paramIds ");
            out_int_list(out, &sp.paramIds);
            w!(out, " :plan_id {}", sp.plan_id);
            w!(out, " :plan_name ");
            out_str(out, sp.plan_name);
            w!(out, " :firstColType {}", sp.firstColType);
            w!(out, " :firstColTypmod {}", sp.firstColTypmod);
            w!(out, " :firstColCollation {}", sp.firstColCollation);
            w!(out, " :useHashTable ");
            out_bool(out, sp.useHashTable);
            w!(out, " :unknownEqFalse ");
            out_bool(out, sp.unknownEqFalse);
            w!(out, " :parallel_safe ");
            out_bool(out, sp.parallel_safe);
            w!(out, " :setParam ");
            out_int_list(out, &sp.setParam);
            w!(out, " :parParam ");
            out_int_list(out, &sp.parParam);
            w!(out, " :args ");
            out_list(out, &sp.args)?;
            w!(out, " :startup_cost {}", sp.startup_cost);
            w!(out, " :per_call_cost {}", sp.per_call_cost);
            w!(out, "}}");
        }
        NodeTag::T_AlternativeSubPlan => {
            let a = node
                .as_variant::<types_nodes::primnodes::AlternativeSubPlan>()
                .expect("AlternativeSubPlan");
            w!(out, "{{ALTERNATIVESUBPLAN :subplans ");
            out_list(out, &a.subplans)?;
            w!(out, "}}");
        }
        NodeTag::T_NotifyStmt => {
            let n = node
                .as_variant::<types_nodes::parsenodes::NotifyStmt>()
                .expect("NotifyStmt");
            w!(out, "{{NOTIFYSTMT :conditionname ");
            out_str(out, n.conditionname);
            w!(out, " :payload ");
            out_str(out, n.payload);
            w!(out, "}}");
        }
        NodeTag::T_WithCheckOption => {
            let w = node
                .as_variant::<types_nodes::parsenodes::WithCheckOption>()
                .expect("WithCheckOption");
            w!(out, "{{WITHCHECKOPTION :kind {}", w.kind as u32);
            w!(out, " :relname ");
            out_str(out, w.relname);
            w!(out, " :polname ");
            out_str(out, w.polname);
            w!(out, " :qual ");
            out_opt_node(out, w.qual)?;
            w!(out, " :cascaded ");
            out_bool(out, w.cascaded);
            w!(out, "}}");
        }
        other => panic!(
            "outNode (outfuncs.c): {other:?} write arm unported (DEFAULT/CHECK + view \
             SELECT-rule sets)"
        ),
    }
    Ok(())
}

fn out_list(out: &mut PgString<'_>, list: &NodeList<'_>) -> PgResult<()> {
    if list.is_nil() {
        w!(out, "<>");
        return Ok(());
    }
    w!(out, "(");
    for (i, item) in list.iter().enumerate() {
        if i > 0 {
            w!(out, " ");
        }
        out_node(out, item)?;
    }
    w!(out, ")");
    Ok(())
}

fn out_bitmapset(out: &mut PgString<'_>, bms: &Bitmapset<'_>) {
    w!(out, "(b");
    for m in bms.iter() {
        w!(out, " {m}");
    }
    w!(out, ")");
}

fn out_bool(out: &mut PgString<'_>, b: bool) {
    w!(out, "{}", if b { "true" } else { "false" });
}

fn out_var(out: &mut PgString<'_>, v: &Var<'_>) {
    w!(
        out,
        "{{VAR :varno {} :varattno {} :vartype {} :vartypmod {} :varcollid {} :varnullingrels ",
        v.varno, v.varattno, v.vartype, v.vartypmod, v.varcollid
    );
    out_bitmapset(out, &v.varnullingrels);
    w!(
        out,
        " :varlevelsup {} :varreturningtype {} :varnosyn {} :varattnosyn {} :location -1}}",
        v.varlevelsup, v.varreturningtype as u32, v.varnosyn, v.varattnosyn
    );
}

fn out_place_holder_var(out: &mut PgString<'_>, phv: &PlaceHolderVar<'_>) -> PgResult<()> {
    w!(out, "{{PLACEHOLDERVAR :phexpr ");
    out_node(out, phv.phexpr)?;
    w!(out, " :phrels ");
    out_bitmapset(out, &phv.phrels);
    w!(out, " :phnullingrels ");
    out_bitmapset(out, &phv.phnullingrels);
    w!(out, " :phid {} :phlevelsup {}}}", phv.phid, phv.phlevelsup);
    Ok(())
}

fn out_const(out: &mut PgString<'_>, c: &Const) {
    w!(
        out,
        "{{CONST :consttype {} :consttypmod {} :constcollid {} :constlen {} :constbyval ",
        c.consttype, c.consttypmod, c.constcollid, c.constlen
    );
    out_bool(out, c.constbyval);
    w!(out, " :constisnull ");
    out_bool(out, c.constisnull);
    w!(out, " :location -1 :constvalue ");
    if c.constisnull {
        w!(out, "<>");
    } else {
        out_datum(out, c.constvalue, c.constlen, c.constbyval);
    }
    w!(out, "}}");
}

// _outDatum (outfuncs.c) prints bytes as `char`, unsigned on aarch64 Linux —
// the byte-compare oracle; readfuncs accepts either signedness.
fn out_datum(out: &mut PgString<'_>, value: Datum, typlen: i32, typbyval: bool) {
    if typbyval {
        // The full 8-byte Datum word: SIZEOF_DATUM is pinned to 8 on every
        // target and readDatum consumes exactly 8 byte tokens. as_usize()
        // emits only 4 bytes on wasm32 and the reader then dies on "]".
        let bytes = value.as_u64().to_le_bytes();
        w!(out, "{} [ ", typlen as u32);
        for b in bytes {
            w!(out, "{b} ");
        }
        w!(out, "]");
        return;
    }
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        w!(out, "0 [ ]");
        return;
    }
    let length = match typlen {
        l if l > 0 => l as usize,
        -1 => {
            // SAFETY: byref const datum points at a live in-line varlena.
            unsafe { varlena_size(p) }
        }
        -2 => {
            // cstring (unknown-type Consts): NUL included, as C's strlen+1.
            let mut n = 0usize;
            // SAFETY: byref cstring datum points at a live NUL-terminated string.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        other => panic!("_outDatum (outfuncs.c): typlen {other} unported"),
    };
    if length == 0 {
        w!(out, "0 [ ]");
        return;
    }
    w!(out, "{length} [ ");
    for i in 0..length {
        // SAFETY: length derived from the datum's own size.
        let b = unsafe { *p.add(i) };
        w!(out, "{b} ");
    }
    w!(out, "]");
}

// VARSIZE_ANY over a plain (parser-built, never toasted) varlena image.
unsafe fn varlena_size(p: *const u8) -> usize {
    // SAFETY: caller guarantees a live varlena header at p.
    let b0 = unsafe { *p };
    if b0 & 0x01 != 0 {
        (b0 as usize) >> 1
    } else {
        // SAFETY: 4-byte header form.
        let word = unsafe { core::ptr::read_unaligned(p as *const u32) };
        (word as usize) >> 2
    }
}

fn out_op_expr(out: &mut PgString<'_>, o: &OpExpr<'_>) -> PgResult<()> {
    w!(
        out,
        "{{OPEXPR :opno {} :opfuncid {} :opresulttype {} :opretset ",
        o.opno, o.opfuncid, o.opresulttype
    );
    out_bool(out, o.opretset);
    w!(out, " :opcollid {} :inputcollid {} :args ", o.opcollid, o.inputcollid);
    out_list(out, &o.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_distinct_expr(
    out: &mut PgString<'_>,
    o: &types_nodes::primnodes::DistinctExpr<'_>,
) -> PgResult<()> {
    w!(
        out,
        "{{DISTINCTEXPR :opno {} :opfuncid {} :opresulttype {} :opretset ",
        o.opno, o.opfuncid, o.opresulttype
    );
    out_bool(out, o.opretset);
    w!(out, " :opcollid {} :inputcollid {} :args ", o.opcollid, o.inputcollid);
    out_list(out, &o.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_null_if_expr(
    out: &mut PgString<'_>,
    o: &types_nodes::primnodes::NullIfExpr<'_>,
) -> PgResult<()> {
    w!(
        out,
        "{{NULLIFEXPR :opno {} :opfuncid {} :opresulttype {} :opretset ",
        o.opno, o.opfuncid, o.opresulttype
    );
    out_bool(out, o.opretset);
    w!(out, " :opcollid {} :inputcollid {} :args ", o.opcollid, o.inputcollid);
    out_list(out, &o.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_func_expr(out: &mut PgString<'_>, f: &FuncExpr<'_>) -> PgResult<()> {
    w!(out, "{{FUNCEXPR :funcid {} :funcresulttype {} :funcretset ", f.funcid, f.funcresulttype);
    out_bool(out, f.funcretset);
    w!(out, " :funcvariadic ");
    out_bool(out, f.funcvariadic);
    w!(
        out,
        " :funcformat {} :funccollid {} :inputcollid {} :args ",
        f.funcformat as u32, f.funccollid, f.inputcollid
    );
    out_list(out, &f.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_named_arg_expr(out: &mut PgString<'_>, n: &NamedArgExpr<'_>) -> PgResult<()> {
    w!(out, "{{NAMEDARGEXPR :arg ");
    out_opt_node(out, n.arg)?;
    w!(out, " :name ");
    out_str(out, n.name);
    w!(out, " :argnumber {} :location -1}}", n.argnumber);
    Ok(())
}

fn out_bool_expr(out: &mut PgString<'_>, b: &BoolExpr<'_>) -> PgResult<()> {
    let opstr = match b.boolop {
        BoolExprType::AND_EXPR => "and",
        BoolExprType::OR_EXPR => "or",
        BoolExprType::NOT_EXPR => "not",
    };
    w!(out, "{{BOOLEXPR :boolop {opstr} :args ");
    out_list(out, &b.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_null_test(out: &mut PgString<'_>, n: &NullTest<'_>) -> PgResult<()> {
    w!(out, "{{NULLTEST :arg ");
    match n.arg {
        Some(arg) => out_node(out, arg)?,
        None => w!(out, "<>"),
    }
    w!(out, " :nulltesttype {} :argisrow ", n.nulltesttype as u32);
    out_bool(out, n.argisrow);
    w!(out, " :location -1}}");
    Ok(())
}

fn out_coerce_to_domain(out: &mut PgString<'_>, c: &CoerceToDomain<'_>) -> PgResult<()> {
    w!(out, "{{COERCETODOMAIN :arg ");
    out_node(out, c.arg)?;
    w!(
        out,
        " :resulttype {} :resulttypmod {} :resultcollid {} :coercionformat {} :location -1}}",
        c.resulttype, c.resulttypmod, c.resultcollid, c.coercionformat as u32
    );
    Ok(())
}

fn out_partition_bound_spec(
    out: &mut PgString<'_>,
    b: &PartitionBoundSpec<'_>,
) -> PgResult<()> {
    w!(out, "{{PARTITIONBOUNDSPEC :strategy ");
    if b.strategy == 0 {
        w!(out, "<>");
    } else {
        w!(out, "{}", b.strategy as char);
    }
    w!(out, " :is_default ");
    out_bool(out, b.is_default);
    w!(out, " :modulus {} :remainder {} :listdatums ", b.modulus, b.remainder);
    out_list(out, &b.listdatums)?;
    w!(out, " :lowerdatums ");
    out_list(out, &b.lowerdatums)?;
    w!(out, " :upperdatums ");
    out_list(out, &b.upperdatums)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_partition_range_datum(
    out: &mut PgString<'_>,
    d: &PartitionRangeDatum<'_>,
) -> PgResult<()> {
    w!(out, "{{PARTITIONRANGEDATUM :kind {} :value ", d.kind as i32);
    match d.value {
        Some(v) => out_node(out, v)?,
        None => w!(out, "<>"),
    }
    w!(out, " :location -1}}");
    Ok(())
}

fn out_relabel_type(out: &mut PgString<'_>, r: &RelabelType<'_>) -> PgResult<()> {
    w!(out, "{{RELABELTYPE :arg ");
    out_node(out, r.arg)?;
    w!(
        out,
        " :resulttype {} :resulttypmod {} :resultcollid {} :relabelformat {} :location -1}}",
        r.resulttype, r.resulttypmod, r.resultcollid, r.relabelformat as u32
    );
    Ok(())
}

fn out_coerce_via_io(out: &mut PgString<'_>, c: &CoerceViaIO<'_>) -> PgResult<()> {
    w!(out, "{{COERCEVIAIO :arg ");
    out_node(out, c.arg)?;
    w!(
        out,
        " :resulttype {} :resultcollid {} :coerceformat {} :location -1}}",
        c.resulttype, c.resultcollid, c.coerceformat as u32
    );
    Ok(())
}

fn out_array_coerce_expr(out: &mut PgString<'_>, a: &ArrayCoerceExpr<'_>) -> PgResult<()> {
    w!(out, "{{ARRAYCOERCEEXPR :arg ");
    out_node(out, a.arg)?;
    w!(out, " :elemexpr ");
    out_opt_node(out, a.elemexpr)?;
    w!(
        out,
        " :resulttype {} :resulttypmod {} :resultcollid {} :coerceformat {} :location -1}}",
        a.resulttype, a.resulttypmod, a.resultcollid, a.coerceformat as u32
    );
    Ok(())
}

fn out_convert_rowtype_expr(out: &mut PgString<'_>, c: &ConvertRowtypeExpr<'_>) -> PgResult<()> {
    w!(out, "{{CONVERTROWTYPEEXPR :arg ");
    out_node(out, c.arg)?;
    w!(
        out,
        " :resulttype {} :convertformat {} :location -1}}",
        c.resulttype, c.convertformat as u32
    );
    Ok(())
}

// outToken (outfuncs.c): backslash-escape anything read.c treats specially.
fn out_token(out: &mut PgString<'_>, s: &str) {
    if s.is_empty() {
        w!(out, "\"\"");
        return;
    }
    let b = s.as_bytes();
    if b[0] == b'<'
        || b[0] == b'"'
        || b[0].is_ascii_digit()
        || ((b[0] == b'+' || b[0] == b'-')
            && b.len() > 1
            && (b[1].is_ascii_digit() || b[1] == b'.'))
    {
        w!(out, "\\");
    }
    for c in s.chars() {
        if matches!(c, ' ' | '\n' | '\t' | '(' | ')' | '{' | '}' | '\\') {
            w!(out, "\\");
        }
        w!(out, "{c}");
    }
}

fn out_str(out: &mut PgString<'_>, s: Option<&str>) {
    match s {
        None => w!(out, "<>"),
        Some(s) => out_token(out, s),
    }
}

// outChar (outfuncs.c): '\0' keeps its traditional <> encoding.
fn out_char(out: &mut PgString<'_>, c: u8) {
    if c == 0 {
        w!(out, "<>");
        return;
    }
    let buf = [c];
    out_token(out, core::str::from_utf8(&buf).expect("outChar ascii"));
}

// _outString (outfuncs.c): always quoted, content escaped via outToken.
fn out_string_node(out: &mut PgString<'_>, s: &str) {
    w!(out, "\"");
    if !s.is_empty() {
        out_token(out, s);
    }
    w!(out, "\"");
}

fn out_json_format(out: &mut PgString<'_>, f: &types_nodes::JsonFormat) {
    w!(
        out,
        "{{JSONFORMAT :format_type {} :encoding {} :location -1}}",
        f.format_type as u32, f.encoding as u32
    );
}

fn out_opt_json_format(out: &mut PgString<'_>, f: Option<&types_nodes::JsonFormat>) {
    match f {
        None => w!(out, "<>"),
        Some(f) => out_json_format(out, f),
    }
}

fn out_json_returning(out: &mut PgString<'_>, r: &types_nodes::JsonReturning<'_>) {
    w!(out, "{{JSONRETURNING :format ");
    out_opt_json_format(out, r.format);
    w!(out, " :typid {} :typmod {}}}", r.typid, r.typmod);
}

fn out_opt_json_returning(out: &mut PgString<'_>, r: Option<&types_nodes::JsonReturning<'_>>) {
    match r {
        None => w!(out, "<>"),
        Some(r) => out_json_returning(out, r),
    }
}

fn out_opt_node(out: &mut PgString<'_>, n: Option<Node<'_>>) -> PgResult<()> {
    match n {
        None => {
            w!(out, "<>");
            Ok(())
        }
        Some(n) => out_node(out, n),
    }
}

fn out_opt_list(out: &mut PgString<'_>, list: &OptNodeList<'_>) -> PgResult<()> {
    if list.is_nil() {
        w!(out, "<>");
        return Ok(());
    }
    w!(out, "(");
    for (i, item) in list.iter().enumerate() {
        if i > 0 {
            w!(out, " ");
        }
        out_opt_node(out, item)?;
    }
    w!(out, ")");
    Ok(())
}

fn out_int_list(out: &mut PgString<'_>, l: &IntList<'_>) {
    if l.is_nil() {
        w!(out, "<>");
        return;
    }
    w!(out, "(i");
    for v in l.iter() {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_oid_list(out: &mut PgString<'_>, l: &OidList<'_>) {
    if l.is_nil() {
        w!(out, "<>");
        return;
    }
    w!(out, "(o");
    for v in l.iter() {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_xml_expr(out: &mut PgString<'_>, x: &XmlExpr<'_>) -> PgResult<()> {
    w!(out, "{{XMLEXPR :op {} :name ", x.op as u32);
    out_str(out, x.name);
    w!(out, " :named_args ");
    out_list(out, &x.named_args)?;
    w!(out, " :arg_names ");
    out_list(out, &x.arg_names)?;
    w!(out, " :args ");
    out_list(out, &x.args)?;
    w!(out, " :xmloption {} :indent ", x.xmloption as u32);
    out_bool(out, x.indent);
    w!(out, " :type {} :typmod {} :location -1}}", x.r#type, x.typmod);
    Ok(())
}

fn out_table_func(out: &mut PgString<'_>, tf: &TableFunc<'_>) -> PgResult<()> {
    w!(out, "{{TABLEFUNC :functype {} :ns_uris ", tf.functype as u32);
    out_list(out, &tf.ns_uris)?;
    w!(out, " :ns_names ");
    out_opt_list(out, &tf.ns_names)?;
    w!(out, " :docexpr ");
    out_opt_node(out, tf.docexpr)?;
    w!(out, " :rowexpr ");
    out_opt_node(out, tf.rowexpr)?;
    w!(out, " :colnames ");
    out_list(out, &tf.colnames)?;
    w!(out, " :coltypes ");
    out_oid_list(out, &tf.coltypes);
    w!(out, " :coltypmods ");
    out_int_list(out, &tf.coltypmods);
    w!(out, " :colcollations ");
    out_oid_list(out, &tf.colcollations);
    w!(out, " :colexprs ");
    out_opt_list(out, &tf.colexprs)?;
    w!(out, " :coldefexprs ");
    out_opt_list(out, &tf.coldefexprs)?;
    w!(out, " :colvalexprs ");
    out_opt_list(out, &tf.colvalexprs)?;
    w!(out, " :passingvalexprs ");
    out_list(out, &tf.passingvalexprs)?;
    w!(out, " :notnulls ");
    out_bitmapset(out, &tf.notnulls);
    w!(out, " :plan ");
    out_opt_node(out, tf.plan)?;
    w!(out, " :ordinalitycol {} :location -1}}", tf.ordinalitycol);
    Ok(())
}

fn out_alias(out: &mut PgString<'_>, a: &Alias<'_>) -> PgResult<()> {
    w!(out, "{{ALIAS :aliasname ");
    out_str(out, a.aliasname);
    w!(out, " :colnames ");
    out_list(out, &a.colnames)?;
    w!(out, "}}");
    Ok(())
}

fn out_opt_alias(out: &mut PgString<'_>, a: Option<&Alias<'_>>) -> PgResult<()> {
    match a {
        None => {
            w!(out, "<>");
            Ok(())
        }
        Some(a) => out_alias(out, a),
    }
}

fn out_query(out: &mut PgString<'_>, q: &Query<'_>) -> PgResult<()> {
    w!(
        out,
        "{{QUERY :commandType {} :querySource {} :canSetTag ",
        q.commandType as u32, q.querySource as u32
    );
    out_bool(out, q.canSetTag);
    w!(out, " :utilityStmt ");
    out_opt_node(out, q.utilityStmt)?;
    w!(out, " :resultRelation {} :hasAggs ", q.resultRelation);
    out_bool(out, q.hasAggs);
    w!(out, " :hasWindowFuncs ");
    out_bool(out, q.hasWindowFuncs);
    w!(out, " :hasTargetSRFs ");
    out_bool(out, q.hasTargetSRFs);
    w!(out, " :hasSubLinks ");
    out_bool(out, q.hasSubLinks);
    w!(out, " :hasDistinctOn ");
    out_bool(out, q.hasDistinctOn);
    w!(out, " :hasRecursive ");
    out_bool(out, q.hasRecursive);
    w!(out, " :hasModifyingCTE ");
    out_bool(out, q.hasModifyingCTE);
    w!(out, " :hasForUpdate ");
    out_bool(out, q.hasForUpdate);
    w!(out, " :hasRowSecurity ");
    out_bool(out, q.hasRowSecurity);
    w!(out, " :hasGroupRTE ");
    out_bool(out, q.hasGroupRTE);
    w!(out, " :isReturn ");
    out_bool(out, q.isReturn);
    w!(out, " :cteList ");
    out_list(out, &q.cteList)?;
    w!(out, " :rtable ");
    out_list(out, &q.rtable)?;
    w!(out, " :rteperminfos ");
    out_list(out, &q.rteperminfos)?;
    w!(out, " :jointree ");
    match q.jointree {
        None => w!(out, "<>"),
        Some(f) => out_from_expr(out, f)?,
    }
    w!(out, " :mergeActionList ");
    out_list(out, &q.mergeActionList)?;
    w!(out, " :mergeTargetRelation {} :mergeJoinCondition ", q.mergeTargetRelation);
    out_opt_node(out, q.mergeJoinCondition)?;
    w!(out, " :targetList ");
    out_list(out, &q.targetList)?;
    w!(out, " :override {} :onConflict ", q.r#override as u32);
    out_opt_node(out, q.onConflict)?;
    w!(out, " :returningOldAlias ");
    out_str(out, q.returningOldAlias);
    w!(out, " :returningNewAlias ");
    out_str(out, q.returningNewAlias);
    w!(out, " :returningList ");
    out_list(out, &q.returningList)?;
    w!(out, " :groupClause ");
    out_list(out, &q.groupClause)?;
    w!(out, " :groupDistinct ");
    out_bool(out, q.groupDistinct);
    w!(out, " :groupingSets ");
    out_list(out, &q.groupingSets)?;
    w!(out, " :havingQual ");
    out_opt_node(out, q.havingQual)?;
    w!(out, " :windowClause ");
    out_list(out, &q.windowClause)?;
    w!(out, " :distinctClause ");
    out_list(out, &q.distinctClause)?;
    w!(out, " :sortClause ");
    out_list(out, &q.sortClause)?;
    w!(out, " :limitOffset ");
    out_opt_node(out, q.limitOffset)?;
    w!(out, " :limitCount ");
    out_opt_node(out, q.limitCount)?;
    w!(out, " :limitOption {} :rowMarks ", q.limitOption as u32);
    out_list(out, &q.rowMarks)?;
    w!(out, " :setOperations ");
    out_opt_node(out, q.setOperations)?;
    w!(out, " :constraintDeps ");
    out_oid_list(out, &q.constraintDeps);
    w!(out, " :withCheckOptions ");
    out_list(out, &q.withCheckOptions)?;
    w!(out, " :stmt_location -1 :stmt_len -1}}");
    Ok(())
}

fn out_range_tbl_function(
    out: &mut PgString<'_>,
    f: &types_nodes::parsenodes::RangeTblFunction<'_>,
) -> PgResult<()> {
    w!(out, "{{RANGETBLFUNCTION :funcexpr ");
    out_opt_node(out, f.funcexpr)?;
    w!(out, " :funccolcount {} :funccolnames ", f.funccolcount);
    out_list(out, &f.funccolnames)?;
    w!(out, " :funccoltypes ");
    out_oid_list(out, &f.funccoltypes);
    w!(out, " :funccoltypmods ");
    out_int_list(out, &f.funccoltypmods);
    w!(out, " :funccolcollations ");
    out_oid_list(out, &f.funccolcollations);
    w!(out, " :funcparams ");
    out_bitmapset(out, &f.funcparams);
    w!(out, "}}");
    Ok(())
}

fn out_range_tbl_entry(out: &mut PgString<'_>, r: &RangeTblEntry<'_>) -> PgResult<()> {
    w!(out, "{{RANGETBLENTRY :alias ");
    out_opt_alias(out, r.alias)?;
    w!(out, " :eref ");
    out_opt_alias(out, r.eref)?;
    w!(out, " :rtekind {}", r.rtekind as u32);
    match r.rtekind {
        RTEKind::RTE_RELATION => {
            w!(out, " :relid {} :inh ", r.relid);
            out_bool(out, r.inh);
            w!(out, " :relkind ");
            out_char(out, r.relkind);
            w!(
                out,
                " :rellockmode {} :perminfoindex {} :tablesample ",
                r.rellockmode, r.perminfoindex
            );
            out_opt_node(out, r.tablesample)?;
        }
        RTEKind::RTE_SUBQUERY => {
            w!(out, " :subquery ");
            match r.subquery {
                None => w!(out, "<>"),
                Some(q) => out_query(out, q)?,
            }
            w!(out, " :security_barrier ");
            out_bool(out, r.security_barrier);
            w!(out, " :relid {} :inh ", r.relid);
            out_bool(out, r.inh);
            w!(out, " :relkind ");
            out_char(out, r.relkind);
            w!(
                out,
                " :rellockmode {} :perminfoindex {}",
                r.rellockmode, r.perminfoindex
            );
        }
        RTEKind::RTE_JOIN => {
            w!(
                out,
                " :jointype {} :joinmergedcols {} :joinaliasvars ",
                r.jointype as u32, r.joinmergedcols
            );
            out_list(out, &r.joinaliasvars)?;
            w!(out, " :joinleftcols ");
            out_int_list(out, &r.joinleftcols);
            w!(out, " :joinrightcols ");
            out_int_list(out, &r.joinrightcols);
            w!(out, " :join_using_alias ");
            out_opt_alias(out, r.join_using_alias)?;
        }
        RTEKind::RTE_FUNCTION => {
            w!(out, " :functions ");
            out_list(out, &r.functions)?;
            w!(out, " :funcordinality ");
            out_bool(out, r.funcordinality);
        }
        RTEKind::RTE_TABLEFUNC => {
            w!(out, " :tablefunc ");
            out_opt_node(out, r.tablefunc)?;
        }
        RTEKind::RTE_VALUES => {
            w!(out, " :values_lists ");
            out_list(out, &r.values_lists)?;
            w!(out, " :coltypes ");
            out_oid_list(out, &r.coltypes);
            w!(out, " :coltypmods ");
            out_int_list(out, &r.coltypmods);
            w!(out, " :colcollations ");
            out_oid_list(out, &r.colcollations);
        }
        RTEKind::RTE_CTE => {
            w!(out, " :ctename ");
            out_str(out, r.ctename);
            w!(out, " :ctelevelsup {} :self_reference ", r.ctelevelsup);
            out_bool(out, r.self_reference);
            w!(out, " :coltypes ");
            out_oid_list(out, &r.coltypes);
            w!(out, " :coltypmods ");
            out_int_list(out, &r.coltypmods);
            w!(out, " :colcollations ");
            out_oid_list(out, &r.colcollations);
        }
        RTEKind::RTE_NAMEDTUPLESTORE => {
            w!(out, " :enrname ");
            out_str(out, r.enrname);
            w!(out, " :enrtuples {} :coltypes ", r.enrtuples);
            out_oid_list(out, &r.coltypes);
            w!(out, " :coltypmods ");
            out_int_list(out, &r.coltypmods);
            w!(out, " :colcollations ");
            out_oid_list(out, &r.colcollations);
            w!(out, " :relid {}", r.relid);
        }
        RTEKind::RTE_GROUP => {
            w!(out, " :groupexprs ");
            out_list(out, &r.groupexprs)?;
        }
        other => panic!(
            "_outRangeTblEntry (outfuncs.c): {other:?} arm unported (view SELECT-rule set)"
        ),
    }
    w!(out, " :lateral ");
    out_bool(out, r.lateral);
    w!(out, " :inFromCl ");
    out_bool(out, r.inFromCl);
    w!(out, " :securityQuals ");
    out_list(out, &r.securityQuals)?;
    w!(out, "}}");
    Ok(())
}

fn out_rte_permission_info(out: &mut PgString<'_>, p: &RTEPermissionInfo<'_>) {
    w!(out, "{{RTEPERMISSIONINFO :relid {} :inh ", p.relid);
    out_bool(out, p.inh);
    w!(
        out,
        " :requiredPerms {} :checkAsUser {} :selectedCols ",
        p.requiredPerms, p.checkAsUser
    );
    out_bitmapset(out, &p.selectedCols);
    w!(out, " :insertedCols ");
    out_bitmapset(out, &p.insertedCols);
    w!(out, " :updatedCols ");
    out_bitmapset(out, &p.updatedCols);
    w!(out, "}}");
}

fn out_from_expr(out: &mut PgString<'_>, f: &FromExpr<'_>) -> PgResult<()> {
    w!(out, "{{FROMEXPR :fromlist ");
    out_list(out, &f.fromlist)?;
    w!(out, " :quals ");
    out_opt_node(out, f.quals)?;
    w!(out, "}}");
    Ok(())
}

fn out_join_expr(out: &mut PgString<'_>, j: &JoinExpr<'_>) -> PgResult<()> {
    w!(out, "{{JOINEXPR :jointype {} :isNatural ", j.jointype as u32);
    out_bool(out, j.isNatural);
    w!(out, " :larg ");
    out_node(out, j.larg)?;
    w!(out, " :rarg ");
    out_node(out, j.rarg)?;
    w!(out, " :usingClause ");
    out_list(out, &j.usingClause)?;
    w!(out, " :join_using_alias ");
    out_opt_alias(out, j.join_using_alias)?;
    w!(out, " :quals ");
    out_opt_node(out, j.quals)?;
    w!(out, " :alias ");
    out_opt_alias(out, j.alias)?;
    w!(out, " :rtindex {}}}", j.rtindex);
    Ok(())
}

fn out_range_tbl_ref(out: &mut PgString<'_>, r: &RangeTblRef) {
    w!(out, "{{RANGETBLREF :rtindex {}}}", r.rtindex);
}

fn out_target_entry(out: &mut PgString<'_>, t: &TargetEntry<'_>) -> PgResult<()> {
    w!(out, "{{TARGETENTRY :expr ");
    out_node(out, t.expr)?;
    w!(out, " :resno {} :resname ", t.resno);
    out_str(out, t.resname);
    w!(
        out,
        " :ressortgroupref {} :resorigtbl {} :resorigcol {} :resjunk ",
        t.ressortgroupref, t.resorigtbl, t.resorigcol
    );
    out_bool(out, t.resjunk);
    w!(out, "}}");
    Ok(())
}

fn out_sort_group_clause(out: &mut PgString<'_>, s: &SortGroupClause) {
    w!(
        out,
        "{{SORTGROUPCLAUSE :tleSortGroupRef {} :eqop {} :sortop {} :reverse_sort ",
        s.tleSortGroupRef, s.eqop, s.sortop
    );
    out_bool(out, s.reverse_sort);
    w!(out, " :nulls_first ");
    out_bool(out, s.nulls_first);
    w!(out, " :hashable ");
    out_bool(out, s.hashable);
    w!(out, "}}");
}

// SIMPLE content is Integer nodes in memory; C stores it as an int list.
fn out_grouping_set(out: &mut PgString<'_>, g: &types_nodes::parsenodes::GroupingSet<'_>) -> PgResult<()> {
    w!(out, "{{GROUPINGSET :kind {} :content ", g.kind as i32);
    if g.kind == types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_SIMPLE && !g.content.is_nil()
    {
        w!(out, "(i");
        for n in g.content.iter() {
            w!(out, " {}", n.as_integer().expect("SIMPLE grouping-set ref").ival);
        }
        w!(out, ")");
    } else {
        out_list(out, &g.content)?;
    }
    w!(out, " :location -1}}");
    Ok(())
}

fn out_aggref(out: &mut PgString<'_>, a: &Aggref<'_>) -> PgResult<()> {
    w!(
        out,
        "{{AGGREF :aggfnoid {} :aggtype {} :aggcollid {} :inputcollid {} :aggtranstype {} \
         :aggargtypes ",
        a.aggfnoid, a.aggtype, a.aggcollid, a.inputcollid, a.aggtranstype
    );
    out_oid_list(out, &a.aggargtypes);
    w!(out, " :aggdirectargs ");
    out_list(out, &a.aggdirectargs)?;
    w!(out, " :args ");
    out_list(out, &a.args)?;
    w!(out, " :aggorder ");
    out_list(out, &a.aggorder)?;
    w!(out, " :aggdistinct ");
    out_list(out, &a.aggdistinct)?;
    w!(out, " :aggfilter ");
    out_opt_node(out, a.aggfilter)?;
    w!(out, " :aggstar ");
    out_bool(out, a.aggstar);
    w!(out, " :aggvariadic ");
    out_bool(out, a.aggvariadic);
    w!(out, " :aggkind ");
    out_char(out, a.aggkind as u8);
    w!(out, " :aggpresorted ");
    out_bool(out, a.aggpresorted);
    w!(
        out,
        " :agglevelsup {} :aggsplit {} :aggno {} :aggtransno {} :location -1}}",
        a.agglevelsup, a.aggsplit, a.aggno, a.aggtransno
    );
    Ok(())
}

fn out_sub_link(out: &mut PgString<'_>, s: &SubLink<'_>) -> PgResult<()> {
    w!(
        out,
        "{{SUBLINK :subLinkType {} :subLinkId {} :testexpr ",
        s.subLinkType as u32, s.subLinkId
    );
    out_opt_node(out, s.testexpr)?;
    w!(out, " :operName ");
    out_list(out, &s.operName)?;
    w!(out, " :subselect ");
    out_node(out, s.subselect)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_common_table_expr(out: &mut PgString<'_>, c: &CommonTableExpr<'_>) -> PgResult<()> {
    w!(out, "{{COMMONTABLEEXPR :ctename ");
    out_str(out, c.ctename);
    w!(out, " :aliascolnames ");
    out_list(out, &c.aliascolnames)?;
    w!(out, " :ctematerialized {} :ctequery ", c.ctematerialized as u32);
    out_opt_node(out, c.ctequery)?;
    w!(out, " :search_clause ");
    out_opt_node(out, c.search_clause)?;
    w!(out, " :cycle_clause ");
    out_opt_node(out, c.cycle_clause)?;
    w!(out, " :location -1 :cterecursive ");
    out_bool(out, c.cterecursive);
    w!(out, " :cterefcount {} :ctecolnames ", c.cterefcount);
    out_list(out, &c.ctecolnames)?;
    w!(out, " :ctecoltypes ");
    out_oid_list(out, &c.ctecoltypes);
    w!(out, " :ctecoltypmods ");
    out_int_list(out, &c.ctecoltypmods);
    w!(out, " :ctecolcollations ");
    out_oid_list(out, &c.ctecolcollations);
    w!(out, "}}");
    Ok(())
}

fn out_cte_search_clause(out: &mut PgString<'_>, s: &types_nodes::parsenodes::CTESearchClause<'_>) -> PgResult<()> {
    w!(out, "{{CTESEARCHCLAUSE :search_col_list ");
    out_list(out, &s.search_col_list)?;
    w!(out, " :search_breadth_first ");
    out_bool(out, s.search_breadth_first);
    w!(out, " :search_seq_column ");
    out_str(out, s.search_seq_column);
    w!(out, " :location -1}}");
    Ok(())
}

fn out_cte_cycle_clause(out: &mut PgString<'_>, c: &types_nodes::parsenodes::CTECycleClause<'_>) -> PgResult<()> {
    w!(out, "{{CTECYCLECLAUSE :cycle_col_list ");
    out_list(out, &c.cycle_col_list)?;
    w!(out, " :cycle_mark_column ");
    out_str(out, c.cycle_mark_column);
    w!(out, " :cycle_mark_value ");
    out_opt_node(out, c.cycle_mark_value)?;
    w!(out, " :cycle_mark_default ");
    out_opt_node(out, c.cycle_mark_default)?;
    w!(out, " :cycle_path_column ");
    out_str(out, c.cycle_path_column);
    w!(
        out,
        " :location -1 :cycle_mark_type {} :cycle_mark_typmod {} :cycle_mark_collation {} \
         :cycle_mark_neop {}}}",
        c.cycle_mark_type, c.cycle_mark_typmod, c.cycle_mark_collation, c.cycle_mark_neop
    );
    Ok(())
}

fn out_notify_stmt(out: &mut PgString<'_>, n: &types_nodes::parsenodes::NotifyStmt<'_>) -> PgResult<()> {
    w!(out, "{{NOTIFYSTMT :conditionname ");
    out_str(out, n.conditionname);
    w!(out, " :payload ");
    out_str(out, n.payload);
    w!(out, "}}");
    Ok(())
}

fn out_next_value_expr(out: &mut PgString<'_>, n: &types_nodes::primnodes::NextValueExpr) -> PgResult<()> {
    w!(out, "{{NEXTVALUEEXPR :seqid {} :typeId {}}}", n.seqid, n.typeId);
    Ok(())
}

fn out_scalar_array_op_expr(out: &mut PgString<'_>, s: &ScalarArrayOpExpr<'_>) -> PgResult<()> {
    w!(
        out,
        "{{SCALARARRAYOPEXPR :opno {} :opfuncid {} :hashfuncid {} :negfuncid {} :useOr ",
        s.opno, s.opfuncid, s.hashfuncid, s.negfuncid
    );
    out_bool(out, s.useOr);
    w!(out, " :inputcollid {} :args ", s.inputcollid);
    out_list(out, &s.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

#[cfg(test)]
mod tests;
