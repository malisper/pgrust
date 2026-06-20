//! Boundary converter: the c2rust raw `*mut Node` parse graph → the repo's
//! owned `types_nodes` parse tree.
//!
//! Every conversion is the uniform 5-rule mapping (docs/types.md, the grammar
//! memory note): `*mut List` → `PgVec<NodePtr>`; `*mut Node` → `Option<NodePtr>`
//! (or required `NodePtr`); typed `*mut Child` → `Option<PgBox<Child>>`; the
//! leading `type_: NodeTag` tag is dropped (the enum arm carries it); `*mut
//! c_char` → `Option<PgString>`. Small C enums (plain `c_uint` typedefs on the
//! raw side) map to the repo's `#[repr(u32)]` enums by their shared C
//! discriminant.
//!
//! F1 covers the DML + expression core. A node tag with no `types_nodes`
//! counterpart yet (the ~148 DDL/utility nodes) hits [`unported`], a loud
//! mirror-PG-and-panic, behind `base_yyparse`.

#![allow(non_snake_case)]

use core::ffi::c_char;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_error::PgResult;

use backend_nodes_types::node_tags as tags;
use pgrust_pg_ffi::{List as RawList, Node as RawNode};

use types_nodes::nodes::{Node, NodePtr};
use types_nodes::parsestmt::RawStmt;

use backend_nodes_types::parsenodes_stmts as cs; // c2rust statement/expr structs
use backend_nodes_types::parsenodes as cp; // c2rust clause structs
use backend_nodes_types::primnodes as cpr; // c2rust primnode structs

use types_nodes::rawnodes as tn; // owned raw-grammar target types
use types_nodes::rawexprnodes as tn_re; // owned raw-grammar Expr-deriving nodes
use types_nodes::primnodes as tn_prim;
use types_nodes::value as tn_val;
use types_nodes::parsenodes as tn_pn; // owned ObjectType/RoleSpecType
use types_nodes::partition as tn_part; // owned PartitionStrategy/RangeDatumKind

// ===========================================================================
// Uniform helpers.
// ===========================================================================

/// `*mut c_char` → `Option<PgString>` (NULL → None). The C string is copied
/// into `mcx`.
fn cstr_opt<'mcx>(mcx: Mcx<'mcx>, s: *mut c_char) -> PgResult<Option<PgString<'mcx>>> {
    if s.is_null() {
        return Ok(None);
    }
    Ok(Some(cstr(mcx, s)?))
}

/// `*const c_char` (non-NULL) → `PgString` in `mcx`.
fn cstr<'mcx>(mcx: Mcx<'mcx>, s: *const c_char) -> PgResult<PgString<'mcx>> {
    let bytes = unsafe { cstr_bytes(s) };
    let text = String::from_utf8_lossy(bytes);
    PgString::from_str_in(&text, mcx)
}

/// Borrow the bytes of a NUL-terminated C string (without the NUL).
unsafe fn cstr_bytes<'a>(s: *const c_char) -> &'a [u8] {
    if s.is_null() {
        return &[];
    }
    let mut len = 0usize;
    while *s.add(len) != 0 {
        len += 1;
    }
    core::slice::from_raw_parts(s.cast::<u8>(), len)
}

/// `*mut Node` → required `NodePtr` (NULL is a corrupt tree the grammar never
/// produces where a child is required: mirror-PG-and-panic).
fn node_req<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<NodePtr<'mcx>> {
    match node_opt(mcx, n)? {
        Some(p) => Ok(p),
        None => panic!("gram converter: required Node child was NULL (corrupt parse tree)"),
    }
}

/// `*mut Node` → `Option<NodePtr>` (NULL → None).
fn node_opt<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Option<NodePtr<'mcx>>> {
    if n.is_null() {
        return Ok(None);
    }
    let node = convert_node(mcx, n)?;
    Ok(Some(mcx::alloc_in(mcx, node)?))
}

/// `*mut List` of `*mut Node` → `PgVec<NodePtr>` (NULL list → empty vec).
fn node_list<'mcx>(mcx: Mcx<'mcx>, l: *mut RawList) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        out.push(node_req(mcx, np)?);
    }
    Ok(out)
}

/// `ObjectWithArgs.objargs` / `objfuncargs` (`*mut List` of `*mut Node`) →
/// `PgVec<NodePtr>`, preserving the grammar's NULL list cell that encodes a
/// unary operator's missing operand (`NONE`). The C grammar builds e.g.
/// `DROP OPERATOR # (NONE, type)` as `list_make2(NULL, $4)` (gram.y:9106-9108) —
/// a `List *` whose first cell is a raw NULL pointer. C `List` legitimately
/// holds NULL cells; the owned `NodePtr` cannot be NULL, so the NULL cell is
/// represented as an empty `Node::List` sentinel. Every `objargs`/`objfuncargs`
/// consumer reads each cell via `.as_typename()` (parse_oper.c `LookupOperWithArgs`
/// `linitial_node(TypeName,…)`, dropcmds.c `lfirst_node(TypeName,l) != NULL`),
/// which returns `None`/`InvalidOid` for any non-`TypeName` cell — exactly the
/// NONE-operand semantics. (Same NULL-cell representation as
/// [`distinct_clause_list`].) Used ONLY for the two `ObjectWithArgs` lists that
/// C may build with NULL cells; all other required-child lists keep [`node_req`]'s
/// strict NULL check.
fn node_list_nullable<'mcx>(
    mcx: Mcx<'mcx>,
    l: *mut RawList,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the NONE operand: encode as empty Node::List, which
            // `.as_typename()` treats as None (→ InvalidOid) in every consumer.
            None => out.push(mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
        }
    }
    Ok(out)
}

/// `SelectStmt.distinctClause` (`*mut List` of `*mut Node`) → `PgVec<NodePtr>`,
/// preserving the grammar's `list_make1(NIL)` "SELECT DISTINCT (all columns)"
/// marker. The C grammar encodes plain DISTINCT (vs DISTINCT ON) as a one-element
/// list whose sole cell is NULL; the owned model — whose `NodePtr` cannot be NULL
/// — represents that NULL cell as an empty `Node::List`, which the analyze layer
/// (`distinct_all_marker`) detects. Real DISTINCT ON elements are column
/// expressions and convert normally. (analyze/select.c `transformDistinctClause`
/// reads `linitial(distinctClause) == NULL`.)
fn distinct_clause_list<'mcx>(
    mcx: Mcx<'mcx>,
    l: *mut RawList,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the plain-DISTINCT marker: encode as empty Node::List.
            None => out.push(mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
        }
    }
    Ok(out)
}

/// `DefineStmt.args` (`*mut List`) → `PgVec<NodePtr>`, preserving the grammar's
/// `aggr_args` NULL-cell convention. For a new-style aggregate the grammar builds
/// `args` as the pair `list_make2(direct_args_or_NIL, makeInteger(numDirectArgs))`
/// (gram.y `aggr_args`); a zero-direct-argument aggregate (`CREATE AGGREGATE
/// foo(*)`) has `NIL` as the first element, i.e. a NULL list cell. The owned model
/// — whose `NodePtr` cannot be NULL — represents that NULL cell as an empty
/// `Node::List`, which `aggregatecmds::DefineAggregate` reads back through
/// `nodeAsList(&args[0])` (an empty list == no direct args), exactly as C's
/// `linitial_node(List, args)` yields `NIL`. Non-aggregate DefineStmt kinds never
/// place a NULL cell in `args`, so this is a faithful superset of `node_list`.
fn define_args_list<'mcx>(
    mcx: Mcx<'mcx>,
    l: *mut RawList,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the new-style `aggr_args` "no direct args" marker
            // (`NIL` first element); encode as an empty `Node::List`.
            None => out.push(mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
        }
    }
    Ok(out)
}

/// `*mut List` of `Oid` (int cells) → `PgVec<Oid>`.
fn oid_list<'mcx>(mcx: Mcx<'mcx>, l: *mut RawList) -> PgResult<PgVec<'mcx, u32>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        out.push(cell.oid());
    }
    Ok(out)
}

/// `*mut List` of `int` cells → `PgVec<i32>`.
fn int_list<'mcx>(mcx: Mcx<'mcx>, l: *mut RawList) -> PgResult<PgVec<'mcx, i32>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        out.push(cell.int());
    }
    Ok(out)
}

/// Convert a typed `*mut Child` whose conversion fn is `f` → `Option<PgBox<O>>`.
fn child_opt<'mcx, C, O>(
    mcx: Mcx<'mcx>,
    p: *mut C,
    f: impl FnOnce(Mcx<'mcx>, *mut C) -> PgResult<O>,
) -> PgResult<Option<PgBox<'mcx, O>>> {
    if p.is_null() {
        return Ok(None);
    }
    let v = f(mcx, p)?;
    Ok(Some(mcx::alloc_in(mcx, v)?))
}

/// Convert a typed `*mut Child` (whose owned form is a [`Node`] arm) by
/// reinterpreting it as a `*mut RawNode` and routing through [`convert_node`]
/// (NULL → None). The child struct begins with a `NodeTag`, so this dispatches
/// on the tag exactly as the C tree links these sub-nodes by `Node *`.
fn child_node_opt<'mcx, C>(mcx: Mcx<'mcx>, p: *mut C) -> PgResult<Option<NodePtr<'mcx>>> {
    node_opt(mcx, p.cast::<RawNode>())
}

/// A loud mirror-PG-and-panic for a parse node whose `types_nodes` type is not
/// yet authored (the ~148 DDL/utility nodes — parser grammar F2+).
fn unported(tag: u32, name: &str) -> ! {
    panic!(
        "gram converter: node tag {tag} ({name}) conversion not yet ported \
         (DDL/utility node; parser grammar F2+)"
    );
}

// ===========================================================================
// Top-level dispatch.
// ===========================================================================

/// Convert a raw list element into the owned [`RawStmt`].
///
/// In `RAW_PARSE_DEFAULT` mode the grammar wraps each statement in a `RawStmt`
/// (`stmtmulti`), so the cell is a `*mut RawStmt`. But the non-default
/// `RawParseMode`s build `parsetree = list_make1($n)` over a *bare* node:
/// `MODE_TYPE_NAME Typename` yields a `TypeName *` cell (gram.y:920), and the
/// PL/pgSQL expression/assignment modes yield bare expression cells. Those cells
/// are NOT `RawStmt`s. We dispatch on the cell's leading `NodeTag`: a real
/// `T_RawStmt` is converted field-by-field; any other tag is a bare node, which
/// we convert directly and wrap in a synthetic `RawStmt` (mirroring how callers
/// like `typeStringToTypeName` do `linitial_node(TypeName, list)` on the bare
/// element — the wrapper is transparent because the consumer reads `.stmt`).
pub fn convert_raw_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    rs: *mut cs::RawStmt,
) -> PgResult<RawStmt<'mcx>> {
    let node: *mut RawNode = rs.cast();
    let tag = unsafe { (*node).type_ };
    if tag == tags::T_RawStmt {
        let rs = unsafe { &*rs };
        return Ok(RawStmt {
            stmt: node_req(mcx, rs.stmt)?,
            stmt_location: rs.stmt_location,
            stmt_len: rs.stmt_len,
        });
    }
    // Bare node (RAW_PARSE_TYPE_NAME / RAW_PARSE_PLPGSQL_*): wrap it.
    Ok(RawStmt {
        stmt: node_req(mcx, node)?,
        stmt_location: 0,
        stmt_len: 0,
    })
}

/// Convert any `*mut Node` (dispatch on the leading `type_` tag).
pub fn convert_node<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Node<'mcx>> {
    let tag = unsafe { (*n).type_ };
    match tag {
        // A bare `List *` node (e.g. a VALUES row): convert its elements.
        tags::T_List => {
            let l = n.cast::<RawList>();
            Ok(Node::mk_list(mcx, node_list(mcx, l)?)?)
        }

        // --- statements ---
        tags::T_SelectStmt => Ok(Node::mk_select_stmt(mcx, conv_select(mcx, n.cast())?)?),
        tags::T_InsertStmt => Ok(Node::mk_insert_stmt(mcx, conv_insert(mcx, n.cast())?)?),
        tags::T_UpdateStmt => Ok(Node::mk_update_stmt(mcx, conv_update(mcx, n.cast())?)?),
        tags::T_DeleteStmt => Ok(Node::mk_delete_stmt(mcx, conv_delete(mcx, n.cast())?)?),
        tags::T_MergeStmt => Ok(Node::mk_merge_stmt(mcx, conv_merge(mcx, n.cast())?)?),
        tags::T_SetOperationStmt => {
            Ok(Node::mk_set_operation_stmt(mcx, conv_setop_stmt(mcx, n.cast())?)?)
        }

        // --- raw expression / grammar nodes ---
        tags::T_A_Expr => Ok(Node::mk_a_expr(mcx, conv_a_expr(mcx, n.cast())?)?),
        tags::T_ColumnRef => Ok(Node::mk_column_ref(mcx, conv_columnref(mcx, n.cast())?)?),
        tags::T_ParamRef => Ok(Node::mk_param_ref(mcx, conv_paramref(n.cast()))?),
        tags::T_A_Const => Ok(conv_a_const(mcx, n.cast())?),
        tags::T_FuncCall => Ok(Node::mk_func_call(mcx, conv_funccall(mcx, n.cast())?)?),
        tags::T_A_Star => Ok(Node::mk_a_star(mcx, tn::A_Star)?),
        tags::T_A_Indices => Ok(Node::mk_a_indices(mcx, conv_a_indices(mcx, n.cast())?)?),
        tags::T_A_Indirection => {
            Ok(Node::mk_a_indirection(mcx, conv_a_indirection(mcx, n.cast())?)?)
        }
        tags::T_A_ArrayExpr => Ok(Node::mk_a_array_expr(mcx, conv_a_arrayexpr(mcx, n.cast())?)?),
        tags::T_ResTarget => Ok(Node::mk_res_target(mcx, conv_restarget(mcx, n.cast())?)?),
        tags::T_MultiAssignRef => {
            Ok(Node::mk_multi_assign_ref(mcx, conv_multiassignref(mcx, n.cast())?)?)
        }
        tags::T_TypeCast => Ok(Node::mk_type_cast(mcx, conv_typecast(mcx, n.cast())?)?),
        tags::T_CollateClause => Ok(Node::mk_collate_clause(mcx, conv_collate(mcx, n.cast())?)?),
        tags::T_SortBy => Ok(Node::mk_sort_by(mcx, conv_sortby(mcx, n.cast())?)?),
        tags::T_WindowDef => Ok(Node::mk_window_def(mcx, conv_windowdef(mcx, n.cast())?)?),
        tags::T_RangeSubselect => {
            Ok(Node::mk_range_subselect(mcx, conv_rangesubselect(mcx, n.cast())?)?)
        }
        tags::T_RangeFunction => Ok(Node::mk_range_function(mcx, conv_rangefunction(mcx, n.cast())?)?),
        tags::T_RangeTableSample => {
            Ok(Node::mk_range_table_sample(mcx, conv_rangetablesample(mcx, n.cast())?)?)
        }
        tags::T_TypeName => Ok(Node::mk_type_name(mcx, conv_typename(mcx, n.cast())?)?),
        tags::T_ColumnDef => Ok(Node::mk_column_def(mcx, conv_columndef(mcx, n.cast())?)?),

        // --- range/join structure ---
        tags::T_RangeVar => Ok(Node::mk_range_var(mcx, conv_rangevar(mcx, n.cast())?)?),
        tags::T_JoinExpr => Ok(Node::mk_join_expr(mcx, conv_joinexpr(mcx, n.cast())?)?),
        tags::T_FromExpr => Ok(Node::mk_from_expr(mcx, conv_fromexpr(mcx, n.cast())?)?),
        tags::T_RangeTblRef => Ok(Node::mk_range_tbl_ref(mcx, conv_rangetblref(n.cast()))?),
        tags::T_Alias => Ok(Node::mk_alias(mcx, conv_alias(mcx, n.cast())?)?),

        // --- clauses / specs ---
        tags::T_WithClause => Ok(Node::mk_with_clause(mcx, conv_withclause(mcx, n.cast())?)?),
        tags::T_CommonTableExpr => {
            Ok(Node::mk_common_table_expr(mcx, conv_cte(mcx, n.cast())?)?)
        }
        tags::T_InferClause => Ok(Node::mk_infer_clause(mcx, conv_infer(mcx, n.cast())?)?),
        tags::T_OnConflictClause => {
            Ok(Node::mk_on_conflict_clause(mcx, conv_onconflict_clause(mcx, n.cast())?)?)
        }
        tags::T_MergeWhenClause => {
            Ok(Node::mk_merge_when_clause(mcx, conv_mergewhen(mcx, n.cast())?)?)
        }
        tags::T_ReturningClause => {
            Ok(Node::mk_returning_clause(mcx, conv_returning(mcx, n.cast())?)?)
        }
        tags::T_ReturningOption => {
            Ok(Node::mk_returning_option(mcx, conv_returning_option(mcx, n.cast())?)?)
        }
        tags::T_TriggerTransition => {
            Ok(Node::mk_trigger_transition(mcx, conv_trigger_transition(mcx, n.cast())?)?)
        }
        tags::T_RangeTableFunc => {
            Ok(Node::mk_range_table_func(mcx, conv_range_table_func(mcx, n.cast())?)?)
        }
        tags::T_RangeTableFuncCol => {
            Ok(Node::mk_range_table_func_col(mcx, conv_range_table_func_col(mcx, n.cast())?)?)
        }
        tags::T_GroupingSet => Ok(Node::mk_grouping_set(mcx, conv_groupingset(mcx, n.cast())?)?),
        tags::T_WindowClause => Ok(Node::mk_window_clause(mcx, conv_windowclause(mcx, n.cast())?)?),
        tags::T_SortGroupClause => {
            Ok(Node::mk_sort_group_clause(mcx, conv_sortgroupclause(n.cast()))?)
        }
        tags::T_RowMarkClause => Ok(Node::mk_row_mark_clause(mcx, conv_rowmark(n.cast()))?),
        tags::T_LockingClause => {
            Ok(Node::mk_locking_clause(mcx, conv_lockingclause(mcx, n.cast())?)?)
        }

        // --- value (leaf literal) nodes ---
        tags::T_Integer => Ok(conv_value_node(mcx, n)?),
        tags::T_Float => Ok(conv_value_node(mcx, n)?),
        tags::T_Boolean => Ok(conv_value_node(mcx, n)?),
        tags::T_String => Ok(conv_value_node(mcx, n)?),
        tags::T_BitString => Ok(conv_value_node(mcx, n)?),

        // --- grammar-produced raw `Expr`-deriving nodes (rawexprnodes) ---
        tags::T_BoolExpr => Ok(Node::mk_bool_expr(mcx, conv_boolexpr(mcx, n.cast())?)?),
        tags::T_CaseExpr => Ok(Node::mk_case_expr(mcx, conv_caseexpr(mcx, n.cast())?)?),
        tags::T_CaseWhen => Ok(Node::mk_case_when(mcx, conv_casewhen(mcx, n.cast())?)?),
        tags::T_CoalesceExpr => Ok(Node::mk_coalesce_expr(mcx, conv_coalesceexpr(mcx, n.cast())?)?),
        tags::T_MinMaxExpr => Ok(Node::mk_min_max_expr(mcx, conv_minmaxexpr(mcx, n.cast())?)?),
        tags::T_SubLink => Ok(Node::mk_sub_link(mcx, conv_sublink(mcx, n.cast())?)?),
        tags::T_NullTest => Ok(Node::mk_null_test(mcx, conv_nulltest(mcx, n.cast())?)?),
        tags::T_BooleanTest => Ok(Node::mk_boolean_test(mcx, conv_booleantest(mcx, n.cast())?)?),
        tags::T_RowExpr => Ok(Node::mk_row_expr(mcx, conv_rowexpr(mcx, n.cast())?)?),
        tags::T_GroupingFunc => Ok(Node::mk_grouping_func(mcx, conv_groupingfunc(mcx, n.cast())?)?),
        tags::T_CollateExpr => Ok(Node::mk_collate_expr(mcx, conv_collateexpr(mcx, n.cast())?)?),
        tags::T_SetToDefault => Ok(Node::mk_set_to_default(mcx, conv_settodefault(n.cast()))?),
        tags::T_CurrentOfExpr => {
            Ok(Node::mk_current_of_expr(mcx, conv_currentofexpr(mcx, n.cast())?)?)
        }
        tags::T_NamedArgExpr => Ok(Node::mk_named_arg_expr(mcx, conv_namedargexpr(mcx, n.cast())?)?),
        tags::T_MergeSupportFunc => {
            Ok(Node::mk_merge_support_func(mcx, conv_mergesupportfunc(n.cast()))?)
        }
        tags::T_SQLValueFunction => {
            Ok(Node::mk_sql_value_function(mcx, conv_sqlvaluefunction(n.cast()))?)
        }
        tags::T_XmlExpr => Ok(Node::mk_xml_expr(mcx, conv_xmlexpr(mcx, n.cast())?)?),
        tags::T_XmlSerialize => Ok(Node::mk_xml_serialize(mcx, conv_xmlserialize(mcx, n.cast())?)?),

        // --- SQL/JSON raw-grammar nodes (rawexprnodes) ---
        tags::T_JsonValueExpr => {
            Ok(Node::mk_json_value_expr(mcx, conv_json_value_expr(mcx, n.cast())?)?)
        }
        tags::T_JsonBehavior => {
            Ok(Node::mk_json_behavior(mcx, conv_json_behavior(mcx, n.cast())?)?)
        }
        tags::T_JsonIsPredicate => {
            Ok(Node::mk_json_is_predicate(mcx, conv_json_is_predicate(mcx, n.cast())?)?)
        }
        tags::T_JsonOutput => Ok(Node::mk_json_output(mcx, conv_json_output(mcx, n.cast())?)?),
        tags::T_JsonKeyValue => {
            Ok(Node::mk_json_key_value(mcx, conv_json_key_value(mcx, n.cast())?)?)
        }
        tags::T_JsonArgument => {
            Ok(Node::mk_json_argument(mcx, conv_json_argument(mcx, n.cast())?)?)
        }
        tags::T_JsonObjectConstructor => Ok(Node::mk_json_object_constructor(
            mcx,
            conv_json_object_constructor(mcx, n.cast())?,
        )?),
        tags::T_JsonArrayConstructor => Ok(Node::mk_json_array_constructor(
            mcx,
            conv_json_array_constructor(mcx, n.cast())?,
        )?),
        tags::T_JsonArrayQueryConstructor => Ok(Node::mk_json_array_query_constructor(
            mcx,
            conv_json_array_query_constructor(mcx, n.cast())?,
        )?),
        tags::T_JsonAggConstructor => Ok(Node::mk_json_agg_constructor(
            mcx,
            conv_json_agg_constructor(mcx, n.cast())?,
        )?),
        tags::T_JsonObjectAgg => {
            Ok(Node::mk_json_object_agg(mcx, conv_json_object_agg(mcx, n.cast())?)?)
        }
        tags::T_JsonArrayAgg => {
            Ok(Node::mk_json_array_agg(mcx, conv_json_array_agg(mcx, n.cast())?)?)
        }
        tags::T_JsonParseExpr => {
            Ok(Node::mk_json_parse_expr(mcx, conv_json_parse_expr(mcx, n.cast())?)?)
        }
        tags::T_JsonScalarExpr => {
            Ok(Node::mk_json_scalar_expr(mcx, conv_json_scalar_expr(mcx, n.cast())?)?)
        }
        tags::T_JsonSerializeExpr => Ok(Node::mk_json_serialize_expr(
            mcx,
            conv_json_serialize_expr(mcx, n.cast())?,
        )?),
        tags::T_JsonFuncExpr => {
            Ok(Node::mk_json_func_expr(mcx, conv_json_func_expr(mcx, n.cast())?)?)
        }
        tags::T_JsonTablePathSpec => Ok(Node::mk_json_table_path_spec(
            mcx,
            conv_json_table_path_spec(mcx, n.cast())?,
        )?),
        tags::T_JsonTable => Ok(Node::mk_json_table(mcx, conv_json_table(mcx, n.cast())?)?),
        tags::T_JsonTableColumn => {
            Ok(Node::mk_json_table_column(mcx, conv_json_table_column(mcx, n.cast())?)?)
        }

        // --- DDL "CREATE" family (F2): supporting / helper nodes ---
        tags::T_RoleSpec => Ok(Node::mk_role_spec(mcx, conv_rolespec(mcx, n.cast())?)?),
        tags::T_DefElem => Ok(Node::mk_def_elem(mcx, conv_defelem(mcx, n.cast())?)?),
        tags::T_Constraint => Ok(Node::mk_constraint(mcx, conv_constraint(mcx, n.cast())?)?),
        tags::T_TableLikeClause => {
            Ok(Node::mk_table_like_clause(mcx, conv_tablelikeclause(mcx, n.cast())?)?)
        }
        tags::T_IndexElem => Ok(Node::mk_index_elem(mcx, conv_indexelem(mcx, n.cast())?)?),
        tags::T_FunctionParameter => {
            Ok(Node::mk_function_parameter(mcx, conv_functionparameter(mcx, n.cast())?)?)
        }
        tags::T_ObjectWithArgs => {
            Ok(Node::mk_object_with_args(mcx, conv_objectwithargs(mcx, n.cast())?)?)
        }
        tags::T_AccessPriv => Ok(Node::mk_access_priv(mcx, conv_accesspriv(mcx, n.cast())?)?),
        tags::T_CreateOpClassItem => {
            Ok(Node::mk_create_op_class_item(mcx, conv_createopclassitem(mcx, n.cast())?)?)
        }
        tags::T_StatsElem => Ok(Node::mk_stats_elem(mcx, conv_statselem(mcx, n.cast())?)?),
        tags::T_PartitionElem => {
            Ok(Node::mk_partition_elem(mcx, conv_partitionelem(mcx, n.cast())?)?)
        }
        tags::T_PartitionSpec => {
            Ok(Node::mk_partition_spec(mcx, conv_partitionspec(mcx, n.cast())?)?)
        }
        tags::T_PartitionBoundSpec => {
            Ok(Node::mk_partition_bound_spec(mcx, conv_partitionboundspec(mcx, n.cast())?)?)
        }
        tags::T_PartitionRangeDatum => {
            Ok(Node::mk_partition_range_datum(mcx, conv_partitionrangedatum(mcx, n.cast())?)?)
        }
        tags::T_IntoClause => Ok(Node::mk_into_clause(mcx, conv_intoclause(mcx, n.cast())?)?),

        // --- DDL "CREATE" family (F2): statements ---
        tags::T_CreateStmt => Ok(Node::mk_create_stmt(mcx, conv_createstmt(mcx, n.cast())?)?),
        tags::T_IndexStmt => Ok(Node::mk_index_stmt(mcx, conv_indexstmt(mcx, n.cast())?)?),
        tags::T_CreateSeqStmt => {
            Ok(Node::mk_create_seq_stmt(mcx, conv_createseqstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateStatsStmt => {
            Ok(Node::mk_create_stats_stmt(mcx, conv_createstatsstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateFunctionStmt => {
            Ok(Node::mk_create_function_stmt(mcx, conv_createfunctionstmt(mcx, n.cast())?)?)
        }
        tags::T_DefineStmt => Ok(Node::mk_define_stmt(mcx, conv_definestmt(mcx, n.cast())?)?),
        tags::T_CreateDomainStmt => {
            Ok(Node::mk_create_domain_stmt(mcx, conv_createdomainstmt(mcx, n.cast())?)?)
        }
        tags::T_CompositeTypeStmt => {
            Ok(Node::mk_composite_type_stmt(mcx, conv_compositetypestmt(mcx, n.cast())?)?)
        }
        tags::T_CreateEnumStmt => {
            Ok(Node::mk_create_enum_stmt(mcx, conv_createenumstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateRangeStmt => {
            Ok(Node::mk_create_range_stmt(mcx, conv_createrangestmt(mcx, n.cast())?)?)
        }
        tags::T_ViewStmt => Ok(Node::mk_view_stmt(mcx, conv_viewstmt(mcx, n.cast())?)?),
        tags::T_CreateTableAsStmt => {
            Ok(Node::mk_create_table_as_stmt(mcx, conv_createtableasstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateSchemaStmt => {
            Ok(Node::mk_create_schema_stmt(mcx, conv_createschemastmt(mcx, n.cast())?)?)
        }
        tags::T_CreateExtensionStmt => {
            Ok(Node::mk_create_extension_stmt(mcx, conv_createextensionstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateTrigStmt => {
            Ok(Node::mk_create_trig_stmt(mcx, conv_createtrigstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateRoleStmt => {
            Ok(Node::mk_create_role_stmt(mcx, conv_createrolestmt(mcx, n.cast())?)?)
        }
        tags::T_CreatedbStmt => Ok(Node::mk_createdb_stmt(mcx, conv_createdbstmt(mcx, n.cast())?)?),
        tags::T_CreateCastStmt => {
            Ok(Node::mk_create_cast_stmt(mcx, conv_createcaststmt(mcx, n.cast())?)?)
        }
        tags::T_CreateOpClassStmt => {
            Ok(Node::mk_create_op_class_stmt(mcx, conv_createopclassstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateOpFamilyStmt => {
            Ok(Node::mk_create_op_family_stmt(mcx, conv_createopfamilystmt(mcx, n.cast())?)?)
        }
        tags::T_CreatePLangStmt => {
            Ok(Node::mk_create_p_lang_stmt(mcx, conv_createplangstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateTableSpaceStmt => {
            Ok(Node::mk_create_table_space_stmt(mcx, conv_createtablespacestmt(mcx, n.cast())?)?)
        }
        tags::T_CreateConversionStmt => {
            Ok(Node::mk_create_conversion_stmt(mcx, conv_createconversionstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateAmStmt => Ok(Node::mk_create_am_stmt(mcx, conv_createamstmt(mcx, n.cast())?)?),

        // --- DDL "ALTER/DROP" family (F3): supporting / helper nodes ---
        tags::T_PartitionCmd => Ok(Node::mk_partition_cmd(mcx, conv_partitioncmd(mcx, n.cast())?)?),
        tags::T_ReplicaIdentityStmt => {
            Ok(Node::mk_replica_identity_stmt(mcx, conv_replicaidentitystmt(mcx, n.cast())?)?)
        }
        tags::T_ATAlterConstraint => {
            Ok(Node::mk_at_alter_constraint(mcx, conv_ataltconstraint(mcx, n.cast())?)?)
        }

        // --- DDL "ALTER/DROP" family (F3): statements ---
        tags::T_AlterTableStmt => {
            Ok(Node::mk_alter_table_stmt(mcx, conv_altertablestmt(mcx, n.cast())?)?)
        }
        tags::T_AlterTableCmd => Ok(Node::mk_alter_table_cmd(mcx, conv_altertablecmd(mcx, n.cast())?)?),
        tags::T_AlterCollationStmt => {
            Ok(Node::mk_alter_collation_stmt(mcx, conv_altercollationstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterDomainStmt => {
            Ok(Node::mk_alter_domain_stmt(mcx, conv_alterdomainstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterEnumStmt => Ok(Node::mk_alter_enum_stmt(mcx, conv_alterenumstmt(mcx, n.cast())?)?),
        tags::T_AlterStatsStmt => {
            Ok(Node::mk_alter_stats_stmt(mcx, conv_alterstatsstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterSeqStmt => Ok(Node::mk_alter_seq_stmt(mcx, conv_alterseqstmt(mcx, n.cast())?)?),
        tags::T_AlterOpFamilyStmt => {
            Ok(Node::mk_alter_op_family_stmt(mcx, conv_alteropfamilystmt(mcx, n.cast())?)?)
        }
        tags::T_AlterFunctionStmt => {
            Ok(Node::mk_alter_function_stmt(mcx, conv_alterfunctionstmt(mcx, n.cast())?)?)
        }
        tags::T_DropStmt => Ok(Node::mk_drop_stmt(mcx, conv_dropstmt(mcx, n.cast())?)?),
        tags::T_RenameStmt => Ok(Node::mk_rename_stmt(mcx, conv_renamestmt(mcx, n.cast())?)?),
        tags::T_AlterObjectDependsStmt => {
            Ok(Node::mk_alter_object_depends_stmt(mcx, conv_alterobjectdependsstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterObjectSchemaStmt => {
            Ok(Node::mk_alter_object_schema_stmt(mcx, conv_alterobjectschemastmt(mcx, n.cast())?)?)
        }
        tags::T_AlterOwnerStmt => {
            Ok(Node::mk_alter_owner_stmt(mcx, conv_alterownerstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterOperatorStmt => {
            Ok(Node::mk_alter_operator_stmt(mcx, conv_alteroperatorstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterTypeStmt => Ok(Node::mk_alter_type_stmt(mcx, conv_altertypestmt(mcx, n.cast())?)?),
        tags::T_AlterDefaultPrivilegesStmt => Ok(Node::mk_alter_default_privileges_stmt(mcx, 
            conv_alterdefaultprivilegesstmt(mcx, n.cast())?,
        )?),
        tags::T_AlterRoleStmt => Ok(Node::mk_alter_role_stmt(mcx, conv_alterrolestmt(mcx, n.cast())?)?),
        tags::T_AlterRoleSetStmt => {
            Ok(Node::mk_alter_role_set_stmt(mcx, conv_alterrolesetstmt(mcx, n.cast())?)?)
        }
        tags::T_DropOwnedStmt => Ok(Node::mk_drop_owned_stmt(mcx, conv_dropownedstmt(mcx, n.cast())?)?),
        tags::T_ReassignOwnedStmt => {
            Ok(Node::mk_reassign_owned_stmt(mcx, conv_reassignownedstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterTableSpaceOptionsStmt => Ok(Node::mk_alter_table_space_options_stmt(mcx, 
            conv_altertablespaceoptionsstmt(mcx, n.cast())?,
        )?),
        tags::T_AlterTableMoveAllStmt => {
            Ok(Node::mk_alter_table_move_all_stmt(mcx, conv_altertablemoveallstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterExtensionStmt => {
            Ok(Node::mk_alter_extension_stmt(mcx, conv_alterextensionstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterExtensionContentsStmt => Ok(Node::mk_alter_extension_contents_stmt(mcx, 
            conv_alterextensioncontentsstmt(mcx, n.cast())?,
        )?),
        tags::T_AlterFdwStmt => Ok(Node::mk_alter_fdw_stmt(mcx, conv_alterfdwstmt(mcx, n.cast())?)?),
        tags::T_AlterForeignServerStmt => {
            Ok(Node::mk_alter_foreign_server_stmt(mcx, conv_alterforeignserverstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterUserMappingStmt => {
            Ok(Node::mk_alter_user_mapping_stmt(mcx, conv_alterusermappingstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterPolicyStmt => {
            Ok(Node::mk_alter_policy_stmt(mcx, conv_alterpolicystmt(mcx, n.cast())?)?)
        }
        tags::T_AlterDatabaseStmt => {
            Ok(Node::mk_alter_database_stmt(mcx, conv_alterdatabasestmt(mcx, n.cast())?)?)
        }
        tags::T_AlterDatabaseRefreshCollStmt => Ok(Node::mk_alter_database_refresh_coll_stmt(mcx, 
            conv_alterdatabaserefreshcollstmt(mcx, n.cast())?,
        )?),
        tags::T_AlterDatabaseSetStmt => {
            Ok(Node::mk_alter_database_set_stmt(mcx, conv_alterdatabasesetstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterTSDictionaryStmt => {
            Ok(Node::mk_alter_ts_dictionary_stmt(mcx, conv_altertsdictionarystmt(mcx, n.cast())?)?)
        }
        tags::T_AlterTSConfigurationStmt => Ok(Node::mk_alter_ts_configuration_stmt(mcx, 
            conv_altertsconfigurationstmt(mcx, n.cast())?,
        )?),
        tags::T_AlterPublicationStmt => {
            Ok(Node::mk_alter_publication_stmt(mcx, conv_alterpublicationstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterSubscriptionStmt => {
            Ok(Node::mk_alter_subscription_stmt(mcx, conv_altersubscriptionstmt(mcx, n.cast())?)?)
        }

        // --- utility / GRANT / transaction family (F4) ---
        tags::T_GrantStmt => Ok(Node::mk_grant_stmt(mcx, conv_grantstmt(mcx, n.cast())?)?),
        tags::T_GrantRoleStmt => Ok(Node::mk_grant_role_stmt(mcx, conv_grantrolestmt(mcx, n.cast())?)?),
        tags::T_VariableSetStmt => Ok(Node::mk_variable_set_stmt(mcx, conv_variablesetstmt(mcx, n.cast())?)?),
        tags::T_VariableShowStmt => {
            Ok(Node::mk_variable_show_stmt(mcx, conv_variableshowstmt(mcx, n.cast())?)?)
        }
        tags::T_TransactionStmt => Ok(Node::mk_transaction_stmt(mcx, conv_transactionstmt(mcx, n.cast())?)?),
        tags::T_CopyStmt => Ok(Node::mk_copy_stmt(mcx, conv_copystmt(mcx, n.cast())?)?),
        tags::T_ExplainStmt => Ok(Node::mk_explain_stmt(mcx, conv_explainstmt(mcx, n.cast())?)?),
        tags::T_PrepareStmt => Ok(Node::mk_prepare_stmt(mcx, conv_preparestmt(mcx, n.cast())?)?),
        tags::T_ExecuteStmt => Ok(Node::mk_execute_stmt(mcx, conv_executestmt(mcx, n.cast())?)?),
        tags::T_DeallocateStmt => Ok(Node::mk_deallocate_stmt(mcx, conv_deallocatestmt(mcx, n.cast())?)?),
        tags::T_DeclareCursorStmt => {
            Ok(Node::mk_declare_cursor_stmt(mcx, conv_declarecursorstmt(mcx, n.cast())?)?)
        }
        tags::T_ClosePortalStmt => Ok(Node::mk_close_portal_stmt(mcx, conv_closeportalstmt(mcx, n.cast())?)?),
        tags::T_FetchStmt => Ok(Node::mk_fetch_stmt(mcx, conv_fetchstmt(mcx, n.cast())?)?),
        tags::T_VacuumStmt => Ok(Node::mk_vacuum_stmt(mcx, conv_vacuumstmt(mcx, n.cast())?)?),
        tags::T_VacuumRelation => Ok(Node::mk_vacuum_relation(mcx, conv_vacuumrelation(mcx, n.cast())?)?),
        tags::T_ClusterStmt => Ok(Node::mk_cluster_stmt(mcx, conv_clusterstmt(mcx, n.cast())?)?),
        tags::T_ReindexStmt => Ok(Node::mk_reindex_stmt(mcx, conv_reindexstmt(mcx, n.cast())?)?),
        tags::T_CheckPointStmt => Ok(Node::mk_check_point_stmt(mcx, tdn::CheckPointStmt)?),
        tags::T_DiscardStmt => Ok(Node::mk_discard_stmt(mcx, conv_discardstmt(n.cast()))?),
        tags::T_LockStmt => Ok(Node::mk_lock_stmt(mcx, conv_lockstmt(mcx, n.cast())?)?),
        tags::T_ConstraintsSetStmt => {
            Ok(Node::mk_constraints_set_stmt(mcx, conv_constraintssetstmt(mcx, n.cast())?)?)
        }
        tags::T_LoadStmt => Ok(Node::mk_load_stmt(mcx, conv_loadstmt(mcx, n.cast())?)?),
        tags::T_TruncateStmt => Ok(Node::mk_truncate_stmt(mcx, conv_truncatestmt(mcx, n.cast())?)?),
        tags::T_CommentStmt => Ok(Node::mk_comment_stmt(mcx, conv_commentstmt(mcx, n.cast())?)?),
        tags::T_SecLabelStmt => Ok(Node::mk_sec_label_stmt(mcx, conv_seclabelstmt(mcx, n.cast())?)?),
        tags::T_RuleStmt => Ok(Node::mk_rule_stmt(mcx, conv_rulestmt(mcx, n.cast())?)?),
        tags::T_NotifyStmt => Ok(Node::mk_notify_stmt(mcx, conv_notifystmt(mcx, n.cast())?)?),
        tags::T_ListenStmt => Ok(Node::mk_listen_stmt(mcx, conv_listenstmt(mcx, n.cast())?)?),
        tags::T_UnlistenStmt => Ok(Node::mk_unlisten_stmt(mcx, conv_unlistenstmt(mcx, n.cast())?)?),
        tags::T_DoStmt => Ok(Node::mk_do_stmt(mcx, conv_dostmt(mcx, n.cast())?)?),
        tags::T_CallStmt => Ok(Node::mk_call_stmt(mcx, conv_callstmt(mcx, n.cast())?)?),
        tags::T_RefreshMatViewStmt => {
            Ok(Node::mk_refresh_mat_view_stmt(mcx, conv_refreshmatviewstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterSystemStmt => Ok(Node::mk_alter_system_stmt(mcx, conv_altersystemstmt(mcx, n.cast())?)?),
        tags::T_DropdbStmt => Ok(Node::mk_dropdb_stmt(mcx, conv_dropdbstmt(mcx, n.cast())?)?),
        tags::T_DropRoleStmt => Ok(Node::mk_drop_role_stmt(mcx, conv_droprolestmt(mcx, n.cast())?)?),
        tags::T_DropTableSpaceStmt => {
            Ok(Node::mk_drop_table_space_stmt(mcx, conv_droptablespacestmt(mcx, n.cast())?)?)
        }
        tags::T_CreateFdwStmt => Ok(Node::mk_create_fdw_stmt(mcx, conv_createfdwstmt(mcx, n.cast())?)?),
        tags::T_CreateForeignServerStmt => Ok(Node::mk_create_foreign_server_stmt(mcx, 
            conv_createforeignserverstmt(mcx, n.cast())?,
        )?),
        tags::T_CreateForeignTableStmt => Ok(Node::mk_create_foreign_table_stmt(mcx, 
            conv_createforeigntablestmt(mcx, n.cast())?,
        )?),
        tags::T_CreateUserMappingStmt => {
            Ok(Node::mk_create_user_mapping_stmt(mcx, conv_createusermappingstmt(mcx, n.cast())?)?)
        }
        tags::T_DropUserMappingStmt => {
            Ok(Node::mk_drop_user_mapping_stmt(mcx, conv_dropusermappingstmt(mcx, n.cast())?)?)
        }
        tags::T_ImportForeignSchemaStmt => Ok(Node::mk_import_foreign_schema_stmt(mcx, 
            conv_importforeignschemastmt(mcx, n.cast())?,
        )?),
        tags::T_CreatePolicyStmt => Ok(Node::mk_create_policy_stmt(mcx, conv_createpolicystmt(mcx, n.cast())?)?),
        tags::T_PublicationTable => Ok(Node::mk_publication_table(mcx, conv_publicationtable(mcx, n.cast())?)?),
        tags::T_PublicationObjSpec => {
            Ok(Node::mk_publication_obj_spec(mcx, conv_publicationobjspec(mcx, n.cast())?)?)
        }
        tags::T_CreatePublicationStmt => {
            Ok(Node::mk_create_publication_stmt(mcx, conv_createpublicationstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateSubscriptionStmt => Ok(Node::mk_create_subscription_stmt(mcx, 
            conv_createsubscriptionstmt(mcx, n.cast())?,
        )?),
        tags::T_DropSubscriptionStmt => {
            Ok(Node::mk_drop_subscription_stmt(mcx, conv_dropsubscriptionstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateEventTrigStmt => {
            Ok(Node::mk_create_event_trig_stmt(mcx, conv_createeventtrigstmt(mcx, n.cast())?)?)
        }
        tags::T_AlterEventTrigStmt => {
            Ok(Node::mk_alter_event_trig_stmt(mcx, conv_altereventtrigstmt(mcx, n.cast())?)?)
        }
        tags::T_CreateTransformStmt => {
            Ok(Node::mk_create_transform_stmt(mcx, conv_createtransformstmt(mcx, n.cast())?)?)
        }
        tags::T_ReturnStmt => Ok(Node::mk_return_stmt(mcx, conv_returnstmt(mcx, n.cast())?)?),
        tags::T_PLAssignStmt => Ok(Node::mk_pl_assign_stmt(mcx, conv_plassignstmt(mcx, n.cast())?)?),

        // --- anything else: the absent DDL/utility node families ---
        other => unported(other, node_tag_name(other)),
    }
}

include!("convert_stmts.rs");
include!("convert_exprs.rs");
include!("convert_misc.rs");
include!("convert_ddl.rs");
include!("convert_json.rs");
