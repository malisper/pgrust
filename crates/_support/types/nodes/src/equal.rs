// equalfuncs.c equal(). Per C's generated comparators: location fields and
// CoercionForm fields are never compared, nor are the equal_ignore fields
// (Var.varnosyn/varattnosyn, Query.queryId, Aggref.aggtranstype/aggpresorted).
#![allow(non_snake_case)]

use datum::Datum;
use types_tuple::varatt::varsize_any;

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, NodeList, OidList, XidList};
use crate::node_tree::{BitString, Boolean, Float, Integer, Node, String};
use crate::parsenodes::{
    CommonTableExpr, DeallocateStmt, DefElem, ExecuteStmt, ExplainStmt, FetchStmt, PrepareStmt,
    GroupingSet, Query, RTEPermissionInfo, RangeTblEntry, TransactionStmt, VariableSetStmt, VariableShowStmt,
    WithCheckOption, WithClause,
};
use crate::list::OptNodeList;
use crate::primnodes::{
    Aggref, Alias, ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CaseExpr, CaseTestExpr,
    CaseWhen, CoalesceExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO,
    CollateExpr, Const, ConvertRowtypeExpr, CurrentOfExpr, DistinctExpr, FieldSelect, FieldStore, ReturningExpr, FromExpr, FuncExpr, GroupingFunc, NamedArgExpr,
    NullTest, OpExpr, Param, PlaceHolderVar, RangeTblRef, RangeVar, RelabelType, RowCompareExpr, RowExpr,
    SQLValueFunction, ScalarArrayOpExpr, SubLink, SubPlan, SubscriptingRef, TableFunc, TargetEntry, Var, WindowFunc,
    MergeSupportFunc, WindowFuncRunCondition, XmlExpr,
};
use crate::rawnodes::{
    A_Const, A_Expr, A_Star, CollateClause, ColumnRef, DeleteStmt, DistinctClause, FuncCall,
    InsertStmt, ParamRef, RangeTableFunc, RangeTableFuncCol, RawStmt, ResTarget, SelectStmt,
    SortBy, TypeCast, TypeName, UpdateStmt,
    ValUnion, XmlSerialize,
};
use crate::tags::NodeTag;

pub trait NodeEqual {
    fn node_equal(&self, other: &Self) -> bool;
}

pub fn equal(a: Node<'_>, b: Node<'_>) -> bool {
    if a.ptr_eq(b) {
        return true;
    }
    let tag = a.node_tag();
    if tag != b.node_tag() {
        return false;
    }
    // C: check_stack_depth() — recursion guard unported repo-wide (stack lane).
    macro_rules! cmp {
        ($as_variant:ident) => {
            a.$as_variant().unwrap().node_equal(b.$as_variant().unwrap())
        };
    }
    match tag {
        NodeTag::T_Alias => cmp!(as_alias),
        NodeTag::T_RangeVar => cmp!(as_range_var),
        NodeTag::T_Var => cmp!(as_var),
        NodeTag::T_PlaceHolderVar => cmp!(as_place_holder_var),
        NodeTag::T_Const => cmp!(as_const),
        NodeTag::T_Param => cmp!(as_param),
        NodeTag::T_Aggref => cmp!(as_aggref),
        NodeTag::T_GroupingFunc => cmp!(as_grouping_func),
        NodeTag::T_WindowFunc => cmp!(as_window_func),
        NodeTag::T_WindowFuncRunCondition => cmp!(as_window_func_run_condition),
        NodeTag::T_MergeSupportFunc => cmp!(as_merge_support_func),
        NodeTag::T_GroupingSet => cmp!(as_grouping_set),
        NodeTag::T_TableSampleClause => cmp!(as_table_sample_clause),
        NodeTag::T_RowExpr => cmp!(as_row_expr),
        NodeTag::T_FieldSelect => cmp!(as_field_select),
        NodeTag::T_ReturningExpr => cmp!(as_returning_expr),
        NodeTag::T_FieldStore => cmp!(as_field_store),
        NodeTag::T_RowCompareExpr => cmp!(as_row_compare_expr),
        NodeTag::T_SQLValueFunction => cmp!(as_sql_value_function),
        NodeTag::T_FuncExpr => cmp!(as_func_expr),
        NodeTag::T_NamedArgExpr => cmp!(as_named_arg_expr),
        NodeTag::T_OpExpr => cmp!(as_op_expr),
        NodeTag::T_ScalarArrayOpExpr => cmp!(as_scalar_array_op_expr),
        NodeTag::T_ArrayExpr => cmp!(as_array_expr),
        NodeTag::T_SubLink => cmp!(as_sub_link),
        NodeTag::T_SubPlan => cmp!(as_sub_plan),
        NodeTag::T_SubscriptingRef => cmp!(as_subscripting_ref),
        NodeTag::T_BoolExpr => cmp!(as_bool_expr),
        NodeTag::T_RelabelType => cmp!(as_relabel_type),
        NodeTag::T_CollateExpr => cmp!(as_collate_expr),
        NodeTag::T_CoerceViaIO => cmp!(as_coerce_via_io),
        NodeTag::T_ArrayCoerceExpr => cmp!(as_array_coerce_expr),
        NodeTag::T_ConvertRowtypeExpr => cmp!(as_convert_rowtype_expr),
        NodeTag::T_CoerceToDomain => cmp!(as_coerce_to_domain),
        NodeTag::T_CoerceToDomainValue => cmp!(as_coerce_to_domain_value),
        NodeTag::T_CaseExpr => cmp!(as_case_expr),
        NodeTag::T_CaseWhen => cmp!(as_case_when),
        NodeTag::T_CaseTestExpr => cmp!(as_case_test_expr),
        NodeTag::T_CoalesceExpr => cmp!(as_coalesce_expr),
        NodeTag::T_NullTest => cmp!(as_null_test),
        NodeTag::T_BooleanTest => cmp!(as_boolean_test),
        NodeTag::T_DistinctExpr => cmp!(as_distinct_expr),
        NodeTag::T_NullIfExpr => cmp!(as_null_if_expr),
        NodeTag::T_CollateClause => cmp!(as_collate_clause),
        NodeTag::T_TargetEntry => cmp!(as_target_entry),
        NodeTag::T_RangeTblRef => cmp!(as_range_tbl_ref),
        NodeTag::T_CurrentOfExpr => cmp!(as_current_of_expr),
        NodeTag::T_FromExpr => cmp!(as_from_expr),
        NodeTag::T_Query => cmp!(as_query),
        NodeTag::T_RangeTblEntry => cmp!(as_range_tbl_entry),
        NodeTag::T_RangeTableSample => cmp!(as_range_table_sample),
        NodeTag::T_WithClause => cmp!(as_with_clause),
        NodeTag::T_WithCheckOption => cmp!(as_with_check_option),
        NodeTag::T_CommonTableExpr => cmp!(as_common_table_expr),
        NodeTag::T_RTEPermissionInfo => cmp!(as_rte_permission_info),
        NodeTag::T_SortGroupClause => {
            a.as_sort_group_clause().unwrap() == b.as_sort_group_clause().unwrap()
        }
        NodeTag::T_TransactionStmt => cmp!(as_transaction_stmt),
        NodeTag::T_DefElem => cmp!(as_def_elem),
        NodeTag::T_VariableSetStmt => cmp!(as_variable_set_stmt),
        NodeTag::T_VariableShowStmt => cmp!(as_variable_show_stmt),
        NodeTag::T_ExplainStmt => cmp!(as_explain_stmt),
        NodeTag::T_PrepareStmt => cmp!(as_prepare_stmt),
        NodeTag::T_ExecuteStmt => cmp!(as_execute_stmt),
        NodeTag::T_FetchStmt => cmp!(as_fetch_stmt),
        NodeTag::T_DeallocateStmt => cmp!(as_deallocate_stmt),
        NodeTag::T_RawStmt => cmp!(as_raw_stmt),
        NodeTag::T_SelectStmt => cmp!(as_select_stmt),
        NodeTag::T_InsertStmt => cmp!(as_insert_stmt),
        NodeTag::T_DeleteStmt => cmp!(as_delete_stmt),
        NodeTag::T_UpdateStmt => cmp!(as_update_stmt),
        NodeTag::T_ResTarget => cmp!(as_res_target),
        NodeTag::T_A_Expr => cmp!(as_a_expr),
        NodeTag::T_A_Const => cmp!(as_a_const),
        NodeTag::T_ColumnRef => cmp!(as_column_ref),
        NodeTag::T_ParamRef => cmp!(as_param_ref),
        NodeTag::T_A_Star => true,
        NodeTag::T_SortBy => cmp!(as_sort_by),
        NodeTag::T_FuncCall => cmp!(as_func_call),
        NodeTag::T_XmlExpr => cmp!(as_xml_expr),
        NodeTag::T_TableFunc => cmp!(as_table_func),
        NodeTag::T_RangeTableFunc => cmp!(as_range_table_func),
        NodeTag::T_RangeTableFuncCol => cmp!(as_range_table_func_col),
        NodeTag::T_XmlSerialize => cmp!(as_xml_serialize),
        NodeTag::T_TypeName => cmp!(as_type_name),
        NodeTag::T_TypeCast => cmp!(as_type_cast),
        NodeTag::T_Integer => cmp!(as_integer),
        NodeTag::T_Float => cmp!(as_float),
        NodeTag::T_Boolean => cmp!(as_boolean),
        NodeTag::T_String => cmp!(as_string),
        NodeTag::T_BitString => cmp!(as_bitstring),
        NodeTag::T_List => cmp!(as_list),
        NodeTag::T_IntList => cmp!(as_int_list),
        NodeTag::T_OidList => cmp!(as_oid_list),
        NodeTag::T_XidList => cmp!(as_xid_list),
        NodeTag::T_Bitmapset => cmp!(as_bitmapset),
        NodeTag::T_JsonFormat => cmp!(as_json_format),
        NodeTag::T_JsonReturning => cmp!(as_json_returning),
        NodeTag::T_JsonValueExpr => cmp!(as_json_value_expr),
        NodeTag::T_JsonConstructorExpr => cmp!(as_json_constructor_expr),
        NodeTag::T_JsonIsPredicate => cmp!(as_json_is_predicate),
        NodeTag::T_JsonBehavior => cmp!(as_json_behavior),
        NodeTag::T_JsonExpr => cmp!(as_json_expr),
        NodeTag::T_JsonTablePath => cmp!(as_json_table_path),
        NodeTag::T_JsonTablePathScan => cmp!(as_json_table_path_scan),
        NodeTag::T_JsonTableSiblingJoin => cmp!(as_json_table_sibling_join),
        NodeTag::T_JsonTablePathSpec => cmp!(as_json_table_path_spec),
        NodeTag::T_JsonTable => cmp!(as_json_table),
        NodeTag::T_JsonTableColumn => cmp!(as_json_table_column),
        other => panic!(
            "equal() (equalfuncs.c): node type {other:?} not in the carried vocabulary — \
             unit backend-nodes-equalfuncs"
        ),
    }
}

#[inline]
pub fn equal_opt(a: Option<Node<'_>>, b: Option<Node<'_>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => equal(a, b),
        (None, None) => true,
        _ => false,
    }
}

#[inline]
fn eq_ref<T: NodeEqual>(a: Option<&T>, b: Option<&T>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.node_equal(b),
        (None, None) => true,
        _ => false,
    }
}

// datumIsEqual (datum.c): by-val full-word compare, by-ref byte-image compare,
// no detoast.
fn datum_is_equal(a: Datum, b: Datum, byval: bool, typlen: i32) -> bool {
    if byval {
        return a == b;
    }
    let p1 = a.as_usize() as *const u8;
    let p2 = b.as_usize() as *const u8;
    // SAFETY: a by-ref Const holds a live datum image of the layout constlen
    // describes (makeConst invariant): plain varlena for -1, NUL-terminated
    // cstring for -2, typlen readable bytes otherwise.
    unsafe {
        let s1 = datum_size(p1, typlen);
        let s2 = datum_size(p2, typlen);
        s1 == s2 && core::slice::from_raw_parts(p1, s1) == core::slice::from_raw_parts(p2, s2)
    }
}

// # Safety: `p` points at a live datum image of the layout `typlen` describes.
unsafe fn datum_size(p: *const u8, typlen: i32) -> usize {
    match typlen {
        -1 => unsafe { varsize_any(p) },
        -2 => {
            let mut n = 0usize;
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    }
}

impl NodeEqual for NodeList<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.len() == b.len() && self.iter().zip(b.iter()).all(|(x, y)| equal(x, y))
    }
}

impl NodeEqual for IntList<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.as_slice() == b.as_slice()
    }
}

impl NodeEqual for OidList<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.as_slice() == b.as_slice()
    }
}

impl NodeEqual for XidList<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.as_slice() == b.as_slice()
    }
}

impl NodeEqual for Bitmapset<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.equal(b)
    }
}

impl NodeEqual for Integer {
    fn node_equal(&self, b: &Self) -> bool {
        self.ival == b.ival
    }
}

impl NodeEqual for Float<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.fval == b.fval
    }
}

impl NodeEqual for Boolean {
    fn node_equal(&self, b: &Self) -> bool {
        self.boolval == b.boolval
    }
}

impl NodeEqual for String<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.sval == b.sval
    }
}

impl NodeEqual for BitString<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.bsval == b.bsval
    }
}

impl NodeEqual for Alias<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.aliasname == b.aliasname && self.colnames.node_equal(&b.colnames)
    }
}

impl NodeEqual for RangeVar<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.catalogname == b.catalogname
            && self.schemaname == b.schemaname
            && self.relname == b.relname
            && self.inh == b.inh
            && self.relpersistence == b.relpersistence
            && eq_ref(self.alias, b.alias)
    }
}

impl NodeEqual for Var<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.varno == b.varno
            && self.varattno == b.varattno
            && self.vartype == b.vartype
            && self.vartypmod == b.vartypmod
            && self.varcollid == b.varcollid
            && self.varnullingrels.equal(&b.varnullingrels)
            && self.varlevelsup == b.varlevelsup
            && self.varreturningtype == b.varreturningtype
    }
}

// C: phexpr and phrels are equal_ignore (pathnodes.h).
impl NodeEqual for PlaceHolderVar<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.phnullingrels.equal(&b.phnullingrels)
            && self.phid == b.phid
            && self.phlevelsup == b.phlevelsup
    }
}

impl NodeEqual for Const {
    fn node_equal(&self, b: &Self) -> bool {
        if self.consttype != b.consttype
            || self.consttypmod != b.consttypmod
            || self.constcollid != b.constcollid
            || self.constlen != b.constlen
            || self.constisnull != b.constisnull
            || self.constbyval != b.constbyval
        {
            return false;
        }
        // C: all NULL constants of the same type are equal (datumIsEqual
        // cannot take nulls).
        if self.constisnull {
            return true;
        }
        datum_is_equal(self.constvalue, b.constvalue, self.constbyval, self.constlen)
    }
}

impl NodeEqual for Param {
    fn node_equal(&self, b: &Self) -> bool {
        self.paramkind == b.paramkind
            && self.paramid == b.paramid
            && self.paramtype == b.paramtype
            && self.paramtypmod == b.paramtypmod
            && self.paramcollid == b.paramcollid
    }
}

impl NodeEqual for Aggref<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.aggfnoid == b.aggfnoid
            && self.aggtype == b.aggtype
            && self.aggcollid == b.aggcollid
            && self.inputcollid == b.inputcollid
            && self.aggargtypes.node_equal(&b.aggargtypes)
            && self.aggdirectargs.node_equal(&b.aggdirectargs)
            && self.args.node_equal(&b.args)
            && self.aggorder.node_equal(&b.aggorder)
            && self.aggdistinct.node_equal(&b.aggdistinct)
            && equal_opt(self.aggfilter, b.aggfilter)
            && self.aggstar == b.aggstar
            && self.aggvariadic == b.aggvariadic
            && self.aggkind == b.aggkind
            && self.agglevelsup == b.agglevelsup
            && self.aggsplit == b.aggsplit
            && self.aggno == b.aggno
            && self.aggtransno == b.aggtransno
    }
}

// C: refs/cols are equal_ignore; locations never compared.
impl NodeEqual for GroupingFunc<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.args.node_equal(&b.args) && self.agglevelsup == b.agglevelsup
    }
}

impl NodeEqual for GroupingSet<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.kind == b.kind && self.content.node_equal(&b.content)
    }
}

impl NodeEqual for crate::parsenodes::TableSampleClause<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.tsmhandler == b.tsmhandler
            && self.args.node_equal(&b.args)
            && equal_opt(self.repeatable, b.repeatable)
    }
}

// C: row_format is a CoercionForm field (never compared).
impl NodeEqual for SubLink<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.subLinkType == b.subLinkType
            && self.subLinkId == b.subLinkId
            && equal_opt(self.testexpr, b.testexpr)
            && self.operName.node_equal(&b.operName)
            && equal(self.subselect, b.subselect)
    }
}

impl NodeEqual for SubPlan<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.subLinkType == b.subLinkType
            && equal_opt(self.testexpr, b.testexpr)
            && self.paramIds.node_equal(&b.paramIds)
            && self.plan_id == b.plan_id
            && self.plan_name == b.plan_name
            && self.firstColType == b.firstColType
            && self.firstColTypmod == b.firstColTypmod
            && self.firstColCollation == b.firstColCollation
            && self.useHashTable == b.useHashTable
            && self.unknownEqFalse == b.unknownEqFalse
            && self.parallel_safe == b.parallel_safe
            && self.setParam.node_equal(&b.setParam)
            && self.parParam.node_equal(&b.parParam)
            && self.args.node_equal(&b.args)
            && self.startup_cost == b.startup_cost
            && self.per_call_cost == b.per_call_cost
    }
}

impl NodeEqual for RowExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.args.node_equal(&b.args)
            && self.row_typeid == b.row_typeid
            && self.colnames.node_equal(&b.colnames)
    }
}

impl NodeEqual for ReturningExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.retlevelsup == b.retlevelsup && self.retold == b.retold && equal(self.retexpr, b.retexpr)
    }
}

impl NodeEqual for FieldSelect<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && self.fieldnum == b.fieldnum
            && self.resulttype == b.resulttype
            && self.resulttypmod == b.resulttypmod
            && self.resultcollid == b.resultcollid
    }
}

impl NodeEqual for FieldStore<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && self.newvals.node_equal(&b.newvals)
            && self.fieldnums.node_equal(&b.fieldnums)
            && self.resulttype == b.resulttype
    }
}

impl NodeEqual for RowCompareExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.cmptype == b.cmptype
            && self.opnos.node_equal(&b.opnos)
            && self.opfamilies.node_equal(&b.opfamilies)
            && self.inputcollids.node_equal(&b.inputcollids)
            && self.largs.node_equal(&b.largs)
            && self.rargs.node_equal(&b.rargs)
    }
}

impl NodeEqual for WindowFunc<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.winfnoid == b.winfnoid
            && self.wintype == b.wintype
            && self.wincollid == b.wincollid
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
            && equal_opt(self.aggfilter, b.aggfilter)
            && self.runCondition.node_equal(&b.runCondition)
            && self.winref == b.winref
            && self.winstar == b.winstar
            && self.winagg == b.winagg
    }
}

impl NodeEqual for WindowFuncRunCondition<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.opno == b.opno
            && self.inputcollid == b.inputcollid
            && self.wfunc_left == b.wfunc_left
            && equal(self.arg, b.arg)
    }
}

impl NodeEqual for MergeSupportFunc {
    fn node_equal(&self, b: &Self) -> bool {
        self.msftype == b.msftype && self.msfcollid == b.msfcollid
    }
}

impl NodeEqual for FuncExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.funcid == b.funcid
            && self.funcresulttype == b.funcresulttype
            && self.funcretset == b.funcretset
            && self.funcvariadic == b.funcvariadic
            && self.funccollid == b.funccollid
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for NamedArgExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.arg, b.arg) && self.name == b.name && self.argnumber == b.argnumber
    }
}

impl NodeEqual for OpExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        // C: a zero opfuncid (not yet looked up) matches any opfuncid.
        self.opno == b.opno
            && (self.opfuncid == b.opfuncid || self.opfuncid == 0 || b.opfuncid == 0)
            && self.opresulttype == b.opresulttype
            && self.opretset == b.opretset
            && self.opcollid == b.opcollid
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for OptNodeList<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.len() == b.len()
            && self.iter().zip(b.iter()).all(|(x, y)| match (x, y) {
                (None, None) => true,
                (Some(x), Some(y)) => equal(x, y),
                _ => false,
            })
    }
}

impl NodeEqual for ScalarArrayOpExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        // C: zero opfuncid/hashfuncid/negfuncid (not yet looked up) match any.
        self.opno == b.opno
            && (self.opfuncid == b.opfuncid || self.opfuncid == 0 || b.opfuncid == 0)
            && (self.hashfuncid == b.hashfuncid || self.hashfuncid == 0 || b.hashfuncid == 0)
            && (self.negfuncid == b.negfuncid || self.negfuncid == 0 || b.negfuncid == 0)
            && self.useOr == b.useOr
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for ArrayExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.array_typeid == b.array_typeid
            && self.array_collid == b.array_collid
            && self.element_typeid == b.element_typeid
            && self.elements.node_equal(&b.elements)
            && self.multidims == b.multidims
    }
}

impl NodeEqual for SubscriptingRef<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.refcontainertype == b.refcontainertype
            && self.refelemtype == b.refelemtype
            && self.refrestype == b.refrestype
            && self.reftypmod == b.reftypmod
            && self.refcollid == b.refcollid
            && self.refupperindexpr.node_equal(&b.refupperindexpr)
            && self.reflowerindexpr.node_equal(&b.reflowerindexpr)
            && equal_opt_pair(self.refexpr, b.refexpr)
            && equal_opt_pair(self.refassgnexpr, b.refassgnexpr)
    }
}

fn equal_opt_pair(a: Option<Node<'_>>, b: Option<Node<'_>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => equal(a, b),
        _ => false,
    }
}

impl NodeEqual for BoolExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.boolop == b.boolop && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for RelabelType<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && self.resulttype == b.resulttype
            && self.resulttypmod == b.resulttypmod
            && self.resultcollid == b.resultcollid
    }
}

impl NodeEqual for CoalesceExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.coalescetype == b.coalescetype
            && self.coalescecollid == b.coalescecollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for CollateExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg) && self.collOid == b.collOid
    }
}

impl NodeEqual for CoerceViaIO<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && self.resulttype == b.resulttype
            && self.resultcollid == b.resultcollid
    }
}

impl NodeEqual for ArrayCoerceExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && equal_opt(self.elemexpr, b.elemexpr)
            && self.resulttype == b.resulttype
            && self.resulttypmod == b.resulttypmod
            && self.resultcollid == b.resultcollid
    }
}

impl NodeEqual for ConvertRowtypeExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg) && self.resulttype == b.resulttype
    }
}

impl NodeEqual for CoerceToDomain<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.arg, b.arg)
            && self.resulttype == b.resulttype
            && self.resulttypmod == b.resulttypmod
            && self.resultcollid == b.resultcollid
    }
}

impl NodeEqual for CoerceToDomainValue {
    fn node_equal(&self, b: &Self) -> bool {
        self.typeId == b.typeId && self.typeMod == b.typeMod && self.collation == b.collation
    }
}

impl NodeEqual for CaseExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.casetype == b.casetype
            && self.casecollid == b.casecollid
            && equal_opt(self.arg, b.arg)
            && self.args.node_equal(&b.args)
            && equal_opt(self.defresult, b.defresult)
    }
}

impl NodeEqual for CaseWhen<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.expr, b.expr) && equal_opt(self.result, b.result)
    }
}

impl NodeEqual for CaseTestExpr {
    fn node_equal(&self, b: &Self) -> bool {
        self.typeId == b.typeId && self.typeMod == b.typeMod && self.collation == b.collation
    }
}

impl NodeEqual for SQLValueFunction {
    fn node_equal(&self, b: &Self) -> bool {
        self.op == b.op && self.r#type == b.r#type && self.typmod == b.typmod
    }
}

impl NodeEqual for NullTest<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.arg, b.arg)
            && self.nulltesttype == b.nulltesttype
            && self.argisrow == b.argisrow
    }
}

impl NodeEqual for XmlExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.op == b.op
            && self.name == b.name
            && self.named_args.node_equal(&b.named_args)
            && self.arg_names.node_equal(&b.arg_names)
            && self.args.node_equal(&b.args)
            && self.xmloption == b.xmloption
            && self.indent == b.indent
            && self.r#type == b.r#type
            && self.typmod == b.typmod
    }
}

impl NodeEqual for TableFunc<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.functype == b.functype
            && self.ns_uris.node_equal(&b.ns_uris)
            && self.ns_names.node_equal(&b.ns_names)
            && equal_opt(self.docexpr, b.docexpr)
            && equal_opt(self.rowexpr, b.rowexpr)
            && self.colnames.node_equal(&b.colnames)
            && self.coltypes.node_equal(&b.coltypes)
            && self.coltypmods.node_equal(&b.coltypmods)
            && self.colcollations.node_equal(&b.colcollations)
            && self.colexprs.node_equal(&b.colexprs)
            && self.coldefexprs.node_equal(&b.coldefexprs)
            && self.colvalexprs.node_equal(&b.colvalexprs)
            && self.passingvalexprs.node_equal(&b.passingvalexprs)
            && self.notnulls.equal(&b.notnulls)
            && equal_opt(self.plan, b.plan)
            && self.ordinalitycol == b.ordinalitycol
    }
}

impl NodeEqual for RangeTableFunc<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.lateral == b.lateral
            && equal_opt(self.docexpr, b.docexpr)
            && equal_opt(self.rowexpr, b.rowexpr)
            && self.namespaces.node_equal(&b.namespaces)
            && self.columns.node_equal(&b.columns)
            && eq_ref(self.alias, b.alias)
    }
}

impl NodeEqual for RangeTableFuncCol<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.colname == b.colname
            && equal_opt(self.typeName, b.typeName)
            && self.for_ordinality == b.for_ordinality
            && self.is_not_null == b.is_not_null
            && equal_opt(self.colexpr, b.colexpr)
            && equal_opt(self.coldefexpr, b.coldefexpr)
    }
}

impl NodeEqual for XmlSerialize<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.xmloption == b.xmloption
            && equal_opt(self.expr, b.expr)
            && equal_opt(self.typeName, b.typeName)
            && self.indent == b.indent
    }
}

impl NodeEqual for BooleanTest<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.arg, b.arg) && self.booltesttype == b.booltesttype
    }
}

impl NodeEqual for DistinctExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.opno == b.opno
            && (self.opfuncid == b.opfuncid || self.opfuncid == 0 || b.opfuncid == 0)
            && self.opresulttype == b.opresulttype
            && self.opretset == b.opretset
            && self.opcollid == b.opcollid
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for crate::primnodes::NullIfExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.opno == b.opno
            && (self.opfuncid == b.opfuncid || self.opfuncid == 0 || b.opfuncid == 0)
            && self.opresulttype == b.opresulttype
            && self.opretset == b.opretset
            && self.opcollid == b.opcollid
            && self.inputcollid == b.inputcollid
            && self.args.node_equal(&b.args)
    }
}

impl NodeEqual for CollateClause<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.arg, b.arg) && self.collname.node_equal(&b.collname)
    }
}

impl NodeEqual for TargetEntry<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal(self.expr, b.expr)
            && self.resno == b.resno
            && self.resname == b.resname
            && self.ressortgroupref == b.ressortgroupref
            && self.resorigtbl == b.resorigtbl
            && self.resorigcol == b.resorigcol
            && self.resjunk == b.resjunk
    }
}

impl NodeEqual for RangeTblRef {
    fn node_equal(&self, b: &Self) -> bool {
        self.rtindex == b.rtindex
    }
}

impl NodeEqual for CurrentOfExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.cvarno == b.cvarno
            && self.cursor_name == b.cursor_name
            && self.cursor_param == b.cursor_param
    }
}

impl NodeEqual for FromExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.fromlist.node_equal(&b.fromlist) && equal_opt(self.quals, b.quals)
    }
}

impl NodeEqual for Query<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.commandType == b.commandType
            && self.querySource == b.querySource
            && self.canSetTag == b.canSetTag
            && equal_opt(self.utilityStmt, b.utilityStmt)
            && self.resultRelation == b.resultRelation
            && self.hasAggs == b.hasAggs
            && self.hasWindowFuncs == b.hasWindowFuncs
            && self.hasTargetSRFs == b.hasTargetSRFs
            && self.hasSubLinks == b.hasSubLinks
            && self.hasDistinctOn == b.hasDistinctOn
            && self.hasRecursive == b.hasRecursive
            && self.hasModifyingCTE == b.hasModifyingCTE
            && self.hasForUpdate == b.hasForUpdate
            && self.hasRowSecurity == b.hasRowSecurity
            && self.hasGroupRTE == b.hasGroupRTE
            && self.isReturn == b.isReturn
            && self.cteList.node_equal(&b.cteList)
            && self.rtable.node_equal(&b.rtable)
            && self.rteperminfos.node_equal(&b.rteperminfos)
            && eq_ref(self.jointree, b.jointree)
            && self.mergeActionList.node_equal(&b.mergeActionList)
            && self.mergeTargetRelation == b.mergeTargetRelation
            && equal_opt(self.mergeJoinCondition, b.mergeJoinCondition)
            && self.targetList.node_equal(&b.targetList)
            && self.r#override == b.r#override
            && equal_opt(self.onConflict, b.onConflict)
            && self.returningOldAlias == b.returningOldAlias
            && self.returningNewAlias == b.returningNewAlias
            && self.returningList.node_equal(&b.returningList)
            && self.groupClause.node_equal(&b.groupClause)
            && self.groupDistinct == b.groupDistinct
            && self.groupingSets.node_equal(&b.groupingSets)
            && equal_opt(self.havingQual, b.havingQual)
            && self.windowClause.node_equal(&b.windowClause)
            && self.distinctClause.node_equal(&b.distinctClause)
            && self.sortClause.node_equal(&b.sortClause)
            && equal_opt(self.limitOffset, b.limitOffset)
            && equal_opt(self.limitCount, b.limitCount)
            && self.limitOption == b.limitOption
            && self.rowMarks.node_equal(&b.rowMarks)
            && equal_opt(self.setOperations, b.setOperations)
            && self.constraintDeps.node_equal(&b.constraintDeps)
            && self.withCheckOptions.node_equal(&b.withCheckOptions)
    }
}

impl NodeEqual for crate::rawnodes::RangeTableSample<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.relation, b.relation)
            && self.method.node_equal(&b.method)
            && self.args.node_equal(&b.args)
            && equal_opt(self.repeatable, b.repeatable)
    }
}

impl NodeEqual for RangeTblEntry<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        eq_ref(self.alias, b.alias)
            && eq_ref(self.eref, b.eref)
            && self.rtekind == b.rtekind
            && self.relid == b.relid
            && self.inh == b.inh
            && self.relkind == b.relkind
            && self.rellockmode == b.rellockmode
            && self.perminfoindex == b.perminfoindex
            && equal_opt(self.tablesample, b.tablesample)
            && eq_ref(self.subquery, b.subquery)
            && self.security_barrier == b.security_barrier
            && self.jointype == b.jointype
            && self.joinmergedcols == b.joinmergedcols
            && self.joinaliasvars.node_equal(&b.joinaliasvars)
            && self.joinleftcols.node_equal(&b.joinleftcols)
            && self.joinrightcols.node_equal(&b.joinrightcols)
            && eq_ref(self.join_using_alias, b.join_using_alias)
            && self.functions.node_equal(&b.functions)
            && self.funcordinality == b.funcordinality
            && equal_opt(self.tablefunc, b.tablefunc)
            && self.values_lists.node_equal(&b.values_lists)
            && self.ctename == b.ctename
            && self.ctelevelsup == b.ctelevelsup
            && self.self_reference == b.self_reference
            && self.coltypes.node_equal(&b.coltypes)
            && self.coltypmods.node_equal(&b.coltypmods)
            && self.colcollations.node_equal(&b.colcollations)
            && self.enrname == b.enrname
            && self.enrtuples == b.enrtuples
            && self.groupexprs.node_equal(&b.groupexprs)
            && self.lateral == b.lateral
            && self.inFromCl == b.inFromCl
            && self.securityQuals.node_equal(&b.securityQuals)
    }
}

impl NodeEqual for WithCheckOption<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.kind == b.kind
            && self.relname == b.relname
            && self.polname == b.polname
            && equal_opt(self.qual, b.qual)
            && self.cascaded == b.cascaded
    }
}

impl NodeEqual for WithClause<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.ctes.node_equal(&b.ctes) && self.recursive == b.recursive
    }
}

impl NodeEqual for CommonTableExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.ctename == b.ctename
            && self.aliascolnames.node_equal(&b.aliascolnames)
            && self.ctematerialized == b.ctematerialized
            && equal_opt(self.ctequery, b.ctequery)
            && equal_opt(self.search_clause, b.search_clause)
            && equal_opt(self.cycle_clause, b.cycle_clause)
            && self.cterecursive == b.cterecursive
            && self.cterefcount == b.cterefcount
            && self.ctecolnames.node_equal(&b.ctecolnames)
            && self.ctecoltypes.node_equal(&b.ctecoltypes)
            && self.ctecoltypmods.node_equal(&b.ctecoltypmods)
            && self.ctecolcollations.node_equal(&b.ctecolcollations)
    }
}

impl NodeEqual for RTEPermissionInfo<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.relid == b.relid
            && self.inh == b.inh
            && self.requiredPerms == b.requiredPerms
            && self.checkAsUser == b.checkAsUser
            && self.selectedCols.equal(&b.selectedCols)
            && self.insertedCols.equal(&b.insertedCols)
            && self.updatedCols.equal(&b.updatedCols)
    }
}

impl NodeEqual for TransactionStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.kind == b.kind
            && self.options.node_equal(&b.options)
            && self.savepoint_name == b.savepoint_name
            && self.gid == b.gid
            && self.chain == b.chain
    }
}

impl NodeEqual for DefElem<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.defnamespace == b.defnamespace
            && self.defname == b.defname
            && equal_opt(self.arg, b.arg)
            && self.defaction == b.defaction
    }
}

impl NodeEqual for VariableSetStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.kind == b.kind
            && self.name == b.name
            && self.args.node_equal(&b.args)
            && self.jumble_args == b.jumble_args
            && self.is_local == b.is_local
    }
}

impl NodeEqual for VariableShowStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.name == b.name
    }
}

impl NodeEqual for ExplainStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.query, b.query) && self.options.node_equal(&b.options)
    }
}

impl NodeEqual for PrepareStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.name == b.name
            && self.argtypes.node_equal(&b.argtypes)
            && equal_opt(self.query, b.query)
    }
}

impl NodeEqual for ExecuteStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.name == b.name && self.params.node_equal(&b.params)
    }
}

impl NodeEqual for FetchStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.direction == b.direction
            && self.howMany == b.howMany
            && self.portalname == b.portalname
            && self.ismove == b.ismove
    }
}

impl NodeEqual for DeallocateStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.name == b.name && self.isall == b.isall
    }
}

impl NodeEqual for RawStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.stmt, b.stmt)
    }
}

impl NodeEqual for DistinctClause<'_> {
    // C: plain DISTINCT is list_make1(NIL); the three states map 1:1.
    fn node_equal(&self, b: &Self) -> bool {
        match (self, b) {
            (DistinctClause::None, DistinctClause::None)
            | (DistinctClause::All, DistinctClause::All) => true,
            (DistinctClause::On(x), DistinctClause::On(y)) => x.node_equal(y),
            _ => false,
        }
    }
}

impl NodeEqual for SelectStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.distinctClause.node_equal(&b.distinctClause)
            && equal_opt(self.intoClause, b.intoClause)
            && self.targetList.node_equal(&b.targetList)
            && self.fromClause.node_equal(&b.fromClause)
            && equal_opt(self.whereClause, b.whereClause)
            && self.groupClause.node_equal(&b.groupClause)
            && self.groupDistinct == b.groupDistinct
            && equal_opt(self.havingClause, b.havingClause)
            && self.windowClause.node_equal(&b.windowClause)
            && self.valuesLists.node_equal(&b.valuesLists)
            && self.sortClause.node_equal(&b.sortClause)
            && equal_opt(self.limitOffset, b.limitOffset)
            && equal_opt(self.limitCount, b.limitCount)
            && self.limitOption == b.limitOption
            && self.lockingClause.node_equal(&b.lockingClause)
            && equal_opt(self.withClause, b.withClause)
            && self.op == b.op
            && self.all == b.all
            && eq_ref(self.larg, b.larg)
            && eq_ref(self.rarg, b.rarg)
    }
}

impl NodeEqual for InsertStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.relation, b.relation)
            && self.cols.node_equal(&b.cols)
            && equal_opt(self.selectStmt, b.selectStmt)
            && equal_opt(self.onConflictClause, b.onConflictClause)
            && equal_opt(self.returningClause, b.returningClause)
            && equal_opt(self.withClause, b.withClause)
            && self.r#override == b.r#override
    }
}

impl NodeEqual for DeleteStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.relation, b.relation)
            && self.usingClause.node_equal(&b.usingClause)
            && equal_opt(self.whereClause, b.whereClause)
            && equal_opt(self.returningClause, b.returningClause)
            && equal_opt(self.withClause, b.withClause)
    }
}

impl NodeEqual for UpdateStmt<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.relation, b.relation)
            && self.targetList.node_equal(&b.targetList)
            && equal_opt(self.whereClause, b.whereClause)
            && self.fromClause.node_equal(&b.fromClause)
            && equal_opt(self.returningClause, b.returningClause)
            && equal_opt(self.withClause, b.withClause)
    }
}

impl NodeEqual for ResTarget<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.name == b.name
            && self.indirection.node_equal(&b.indirection)
            && equal_opt(self.val, b.val)
    }
}

impl NodeEqual for A_Expr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.kind == b.kind
            && self.name.node_equal(&b.name)
            && equal_opt(self.lexpr, b.lexpr)
            && equal_opt(self.rexpr, b.rexpr)
    }
}

impl NodeEqual for A_Const<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        match (&self.val, &b.val) {
            (None, None) => true,
            (Some(x), Some(y)) => match (x, y) {
                (ValUnion::Integer(a), ValUnion::Integer(b)) => a.node_equal(b),
                (ValUnion::Float(a), ValUnion::Float(b)) => a.node_equal(b),
                (ValUnion::Boolean(a), ValUnion::Boolean(b)) => a.node_equal(b),
                (ValUnion::String(a), ValUnion::String(b)) => a.node_equal(b),
                (ValUnion::BitString(a), ValUnion::BitString(b)) => a.node_equal(b),
                _ => false,
            },
            _ => false,
        }
    }
}

impl NodeEqual for ColumnRef<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.fields.node_equal(&b.fields)
    }
}

impl NodeEqual for ParamRef {
    fn node_equal(&self, b: &Self) -> bool {
        self.number == b.number
    }
}

impl NodeEqual for A_Star {
    fn node_equal(&self, _b: &Self) -> bool {
        true
    }
}

impl NodeEqual for SortBy<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.node, b.node)
            && self.sortby_dir == b.sortby_dir
            && self.sortby_nulls == b.sortby_nulls
            && self.useOp.node_equal(&b.useOp)
    }
}

impl NodeEqual for FuncCall<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.funcname.node_equal(&b.funcname)
            && self.args.node_equal(&b.args)
            && self.agg_order.node_equal(&b.agg_order)
            && equal_opt(self.agg_filter, b.agg_filter)
            && equal_opt(self.over, b.over)
            && self.agg_within_group == b.agg_within_group
            && self.agg_star == b.agg_star
            && self.agg_distinct == b.agg_distinct
            && self.func_variadic == b.func_variadic
    }
}

impl NodeEqual for TypeName<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.names.node_equal(&b.names)
            && self.typeOid == b.typeOid
            && self.setof == b.setof
            && self.pct_type == b.pct_type
            && self.typmods.node_equal(&b.typmods)
            && self.typemod == b.typemod
            && self.arrayBounds.node_equal(&b.arrayBounds)
    }
}

impl NodeEqual for TypeCast<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.arg, b.arg) && equal_opt(self.typeName, b.typeName)
    }
}

impl NodeEqual for crate::primnodes::JsonFormat {
    fn node_equal(&self, b: &Self) -> bool {
        self.format_type == b.format_type && self.encoding == b.encoding
    }
}

impl NodeEqual for crate::primnodes::JsonReturning<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        eq_ref(self.format, b.format) && self.typid == b.typid && self.typmod == b.typmod
    }
}

impl NodeEqual for crate::primnodes::JsonValueExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.raw_expr, b.raw_expr)
            && equal_opt(self.formatted_expr, b.formatted_expr)
            && eq_ref(self.format, b.format)
    }
}

impl NodeEqual for crate::primnodes::JsonConstructorExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.r#type == b.r#type
            && self.args.node_equal(&b.args)
            && equal_opt(self.func, b.func)
            && equal_opt(self.coercion, b.coercion)
            && eq_ref(self.returning, b.returning)
            && self.absent_on_null == b.absent_on_null
            && self.unique == b.unique
    }
}

impl NodeEqual for crate::primnodes::JsonIsPredicate<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.expr, b.expr)
            && eq_ref(self.format, b.format)
            && self.item_type == b.item_type
            && self.unique_keys == b.unique_keys
    }
}

impl NodeEqual for crate::primnodes::JsonBehavior<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.btype == b.btype && equal_opt(self.expr, b.expr) && self.coerce == b.coerce
    }
}

impl NodeEqual for crate::primnodes::JsonExpr<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.op == b.op
            && self.column_name == b.column_name
            && equal_opt(self.formatted_expr, b.formatted_expr)
            && eq_ref(self.format, b.format)
            && equal_opt(self.path_spec, b.path_spec)
            && eq_ref(self.returning, b.returning)
            && self.passing_names.node_equal(&b.passing_names)
            && self.passing_values.node_equal(&b.passing_values)
            && equal_opt(self.on_empty, b.on_empty)
            && equal_opt(self.on_error, b.on_error)
            && self.use_io_coercion == b.use_io_coercion
            && self.use_json_coercion == b.use_json_coercion
            && self.wrapper == b.wrapper
            && self.omit_quotes == b.omit_quotes
            && self.collation == b.collation
    }
}

impl NodeEqual for crate::primnodes::JsonTablePath<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.value, b.value) && self.name == b.name
    }
}

impl NodeEqual for crate::primnodes::JsonTablePathScan<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.path, b.path)
            && self.errorOnError == b.errorOnError
            && equal_opt(self.child, b.child)
            && self.colMin == b.colMin
            && self.colMax == b.colMax
    }
}

impl NodeEqual for crate::primnodes::JsonTableSiblingJoin<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.lplan, b.lplan) && equal_opt(self.rplan, b.rplan)
    }
}

impl NodeEqual for crate::rawnodes::JsonTablePathSpec<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.string, b.string) && self.name == b.name
    }
}

impl NodeEqual for crate::rawnodes::JsonTable<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        equal_opt(self.context_item, b.context_item)
            && equal_opt(self.pathspec, b.pathspec)
            && self.passing.node_equal(&b.passing)
            && self.columns.node_equal(&b.columns)
            && equal_opt(self.on_error, b.on_error)
            && eq_ref(self.alias, b.alias)
            && self.lateral == b.lateral
    }
}

impl NodeEqual for crate::rawnodes::JsonTableColumn<'_> {
    fn node_equal(&self, b: &Self) -> bool {
        self.coltype == b.coltype
            && self.name == b.name
            && equal_opt(self.typeName, b.typeName)
            && equal_opt(self.pathspec, b.pathspec)
            && eq_ref(self.format, b.format)
            && self.wrapper == b.wrapper
            && self.quotes == b.quotes
            && self.columns.node_equal(&b.columns)
            && equal_opt(self.on_empty, b.on_empty)
            && equal_opt(self.on_error, b.on_error)
    }
}
