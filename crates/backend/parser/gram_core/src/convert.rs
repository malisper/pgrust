//! Boundary converter: the c2rust raw `*mut Node` parse graph → the repo's
//! owned `nodes` parse tree.
//!
//! Every conversion is the uniform 5-rule mapping (docs/types.md, the grammar
//! memory note): `*mut List` → `PgVec<NodePtr>`; `*mut Node` → `Option<NodePtr>`
//! (or required `NodePtr`); typed `*mut Child` → `Option<PgBox<Child>>`; the
//! leading `type_: NodeTag` tag is dropped (the enum arm carries it); `*mut
//! c_char` → `Option<PgString>`. Small C enums (plain `c_uint` typedefs on the
//! raw side) map to the repo's `#[repr(u32)]` enums by their shared C
//! discriminant.
//!
//! F1 covers the DML + expression core. A node tag with no `nodes`
//! counterpart yet (the ~148 DDL/utility nodes) hits [`unported`], a loud
//! mirror-PG-and-panic, behind `base_yyparse`.

#![allow(non_snake_case)]

use core::ffi::c_char;

use ::mcx::{Mcx, PgBox, PgString, PgVec};
use ::types_error::PgResult;

use backend_nodes_types::node_tags as tags;
use ::pg_ffi_fgram::{List as RawList, Node as RawNode};

use ::nodes::nodes::{Node, NodePtr};
use ::nodes::parsestmt::RawStmt;

use backend_nodes_types::parsenodes_stmts as cs; // c2rust statement/expr structs
use backend_nodes_types::parsenodes as cp; // c2rust clause structs
use backend_nodes_types::primnodes as cpr; // c2rust primnode structs

use ::nodes::rawnodes as tn; // owned raw-grammar target types
use ::nodes::rawexprnodes as tn_re; // owned raw-grammar Expr-deriving nodes
use ::nodes::primnodes as tn_prim;
use ::nodes::value as tn_val;
use ::nodes::parsenodes as tn_pn; // owned ObjectType/RoleSpecType
use ::nodes::partition as tn_part; // owned PartitionStrategy/RangeDatumKind

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
    Ok(Some(::mcx::alloc_in(mcx, node)?))
}

/// `*mut List` of `*mut Node` → `PgVec<NodePtr>` (NULL list → empty vec).
fn node_list<'mcx>(mcx: Mcx<'mcx>, l: *mut RawList) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
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
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the NONE operand: encode as empty Node::List, which
            // `.as_typename()` treats as None (→ InvalidOid) in every consumer.
            None => out.push(::mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
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
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the plain-DISTINCT marker: encode as empty Node::List.
            None => out.push(::mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
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
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL cell == the new-style `aggr_args` "no direct args" marker
            // (`NIL` first element); encode as an empty `Node::List`.
            None => out.push(::mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
        }
    }
    Ok(out)
}

/// `CreateFunctionStmt.sql_body` (`*mut Node`) → `Option<NodePtr>`, preserving the
/// grammar's `BEGIN ATOMIC` compound-statement convention. The `routine_body`
/// production stores a compound statement as `(Node *) list_make1($3)` — a
/// single-item `List` whose member is itself the `List` of body statements — so
/// that parse analysis can tell an empty body (`BEGIN ATOMIC END`,
/// `list_make1(NIL)`) apart from no body at all (NULL). For an empty body the
/// inner `List` is `NIL` (a NULL list cell), which the strict [`node_list`] used
/// by [`arm_t_list`] would reject as a corrupt tree. Here we convert the outer
/// `List` ourselves, encoding the NULL inner cell as an empty `Node::List` —
/// exactly what `interpret_sql_body` reads back as `linitial_node(List, ...)`
/// yielding `NIL` (zero body statements). A non-`List` `sql_body` (the `RETURN`
/// form, a single `ReturnStmt`) is passed through unchanged via [`node_opt`].
fn sql_body_opt<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Option<NodePtr<'mcx>>> {
    if n.is_null() {
        return Ok(None);
    }
    // Only the BEGIN ATOMIC form is a List; the RETURN form is a bare statement.
    let tag = unsafe { (*n).type_ };
    if tag != tags::T_List {
        return node_opt(mcx, n);
    }
    let list: &RawList = unsafe { &*n.cast::<RawList>() };
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let np: *mut RawNode = cell.ptr();
        match node_opt(mcx, np)? {
            Some(p) => out.push(p),
            // NULL inner cell == empty BEGIN ATOMIC body (`list_make1(NIL)`):
            // encode as an empty `Node::List`.
            None => out.push(::mcx::alloc_in(mcx, Node::mk_list(mcx, PgVec::new_in(mcx))?)?),
        }
    }
    Ok(Some(::mcx::alloc_in(mcx, Node::mk_list(mcx, out)?)?))
}

/// `*mut List` of `Oid` (int cells) → `PgVec<Oid>`.
fn oid_list<'mcx>(mcx: Mcx<'mcx>, l: *mut RawList) -> PgResult<PgVec<'mcx, u32>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
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
    let mut out = ::mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
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
    Ok(Some(::mcx::alloc_in(mcx, v)?))
}

/// Convert a typed `*mut Child` (whose owned form is a [`Node`] arm) by
/// reinterpreting it as a `*mut RawNode` and routing through [`convert_node`]
/// (NULL → None). The child struct begins with a `NodeTag`, so this dispatches
/// on the tag exactly as the C tree links these sub-nodes by `Node *`.
fn child_node_opt<'mcx, C>(mcx: Mcx<'mcx>, p: *mut C) -> PgResult<Option<NodePtr<'mcx>>> {
    node_opt(mcx, p.cast::<RawNode>())
}

/// A loud mirror-PG-and-panic for a parse node whose `nodes` type is not
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

// ===========================================================================
// Per-arm dispatch thunks.
//
// `convert_node` is mutually recursive with every `conv_*` helper (each child
// `*mut Node` routes back through `convert_node`). A single monolithic `match`
// over ~220 arms forces the debug build to reserve, in ONE `convert_node` stack
// frame, a distinct slot for every arm's large `conv_*` return struct (debug
// builds do not share/overlap match-arm temporaries) — ~800 KB per frame, which
// overflows the stack after only a handful of recursion levels on finite but
// nested parse trees (e.g. a correlated sub-select with a cast over a subscript,
// as psql's `\d` emits). Release is unaffected (it overlaps the slots).
//
// Splitting each arm into its own `#[inline(never)]` thunk keeps `convert_node`'s
// own frame tiny (just the tag dispatch); each large `conv_*` temporary now lives
// in the thunk's separate, small frame, used only while that one arm runs. This
// is purely a frame-size reduction: the call graph, ordering, and results are
// identical to the inlined match.
macro_rules! gram_arm_thunks {
    // The leading `($mcx, $n)` carries the body's parameter identifiers so the
    // generated fn signature binds them in the SAME macro-hygiene context as the
    // captured `$body` tokens (a plain `mcx`/`n` written in the template would be
    // hygienically distinct from the `mcx`/`n` used inside each `$body`).
    ( ($mcx:ident, $n:ident); $( $thunk:ident => $body:expr ; )+ ) => {
        $(
            #[inline(never)]
            fn $thunk<'mcx>($mcx: Mcx<'mcx>, $n: *mut RawNode) -> PgResult<Node<'mcx>> {
                // `mcx`/`n` may be unused by a few constant arms.
                let _ = (&$mcx, &$n);
                $body
            }
        )+
    };
}

gram_arm_thunks! {
    (mcx, n);
    arm_t_list => {
            let l = n.cast::<RawList>();
            Ok(Node::mk_list(mcx, node_list(mcx, l)?)?)
        } ;
    arm_t_selectstmt => Ok(Node::mk_select_stmt(mcx, conv_select(mcx, n.cast())?)?) ;
    arm_t_insertstmt => Ok(Node::mk_insert_stmt(mcx, conv_insert(mcx, n.cast())?)?) ;
    arm_t_updatestmt => Ok(Node::mk_update_stmt(mcx, conv_update(mcx, n.cast())?)?) ;
    arm_t_deletestmt => Ok(Node::mk_delete_stmt(mcx, conv_delete(mcx, n.cast())?)?) ;
    arm_t_mergestmt => Ok(Node::mk_merge_stmt(mcx, conv_merge(mcx, n.cast())?)?) ;
    arm_t_setoperationstmt => {
            Ok(Node::mk_set_operation_stmt(mcx, conv_setop_stmt(mcx, n.cast())?)?)
        } ;
    arm_t_a_expr => Ok(Node::mk_a_expr(mcx, conv_a_expr(mcx, n.cast())?)?) ;
    arm_t_columnref => Ok(Node::mk_column_ref(mcx, conv_columnref(mcx, n.cast())?)?) ;
    arm_t_paramref => Ok(Node::mk_param_ref(mcx, conv_paramref(n.cast()))?) ;
    arm_t_a_const => Ok(conv_a_const(mcx, n.cast())?) ;
    arm_t_funccall => Ok(Node::mk_func_call(mcx, conv_funccall(mcx, n.cast())?)?) ;
    arm_t_a_star => Ok(Node::mk_a_star(mcx, tn::A_Star)?) ;
    arm_t_a_indices => Ok(Node::mk_a_indices(mcx, conv_a_indices(mcx, n.cast())?)?) ;
    arm_t_a_indirection => {
            Ok(Node::mk_a_indirection(mcx, conv_a_indirection(mcx, n.cast())?)?)
        } ;
    arm_t_a_arrayexpr => Ok(Node::mk_a_array_expr(mcx, conv_a_arrayexpr(mcx, n.cast())?)?) ;
    arm_t_restarget => Ok(Node::mk_res_target(mcx, conv_restarget(mcx, n.cast())?)?) ;
    arm_t_multiassignref => {
            Ok(Node::mk_multi_assign_ref(mcx, conv_multiassignref(mcx, n.cast())?)?)
        } ;
    arm_t_typecast => Ok(Node::mk_type_cast(mcx, conv_typecast(mcx, n.cast())?)?) ;
    arm_t_collateclause => Ok(Node::mk_collate_clause(mcx, conv_collate(mcx, n.cast())?)?) ;
    arm_t_sortby => Ok(Node::mk_sort_by(mcx, conv_sortby(mcx, n.cast())?)?) ;
    arm_t_windowdef => Ok(Node::mk_window_def(mcx, conv_windowdef(mcx, n.cast())?)?) ;
    arm_t_rangesubselect => {
            Ok(Node::mk_range_subselect(mcx, conv_rangesubselect(mcx, n.cast())?)?)
        } ;
    arm_t_rangefunction => Ok(Node::mk_range_function(mcx, conv_rangefunction(mcx, n.cast())?)?) ;
    arm_t_rangetablesample => {
            Ok(Node::mk_range_table_sample(mcx, conv_rangetablesample(mcx, n.cast())?)?)
        } ;
    arm_t_typename => Ok(Node::mk_type_name(mcx, conv_typename(mcx, n.cast())?)?) ;
    arm_t_columndef => Ok(Node::mk_column_def(mcx, conv_columndef(mcx, n.cast())?)?) ;
    arm_t_rangevar => Ok(Node::mk_range_var(mcx, conv_rangevar(mcx, n.cast())?)?) ;
    arm_t_joinexpr => Ok(Node::mk_join_expr(mcx, conv_joinexpr(mcx, n.cast())?)?) ;
    arm_t_fromexpr => Ok(Node::mk_from_expr(mcx, conv_fromexpr(mcx, n.cast())?)?) ;
    arm_t_rangetblref => Ok(Node::mk_range_tbl_ref(mcx, conv_rangetblref(n.cast()))?) ;
    arm_t_alias => Ok(Node::mk_alias(mcx, conv_alias(mcx, n.cast())?)?) ;
    arm_t_withclause => Ok(Node::mk_with_clause(mcx, conv_withclause(mcx, n.cast())?)?) ;
    arm_t_commontableexpr => {
            Ok(Node::mk_common_table_expr(mcx, conv_cte(mcx, n.cast())?)?)
        } ;
    arm_t_inferclause => Ok(Node::mk_infer_clause(mcx, conv_infer(mcx, n.cast())?)?) ;
    arm_t_onconflictclause => {
            Ok(Node::mk_on_conflict_clause(mcx, conv_onconflict_clause(mcx, n.cast())?)?)
        } ;
    arm_t_mergewhenclause => {
            Ok(Node::mk_merge_when_clause(mcx, conv_mergewhen(mcx, n.cast())?)?)
        } ;
    arm_t_returningclause => {
            Ok(Node::mk_returning_clause(mcx, conv_returning(mcx, n.cast())?)?)
        } ;
    arm_t_returningoption => {
            Ok(Node::mk_returning_option(mcx, conv_returning_option(mcx, n.cast())?)?)
        } ;
    arm_t_triggertransition => {
            Ok(Node::mk_trigger_transition(mcx, conv_trigger_transition(mcx, n.cast())?)?)
        } ;
    arm_t_rangetablefunc => {
            Ok(Node::mk_range_table_func(mcx, conv_range_table_func(mcx, n.cast())?)?)
        } ;
    arm_t_rangetablefunccol => {
            Ok(Node::mk_range_table_func_col(mcx, conv_range_table_func_col(mcx, n.cast())?)?)
        } ;
    arm_t_groupingset => Ok(Node::mk_grouping_set(mcx, conv_groupingset(mcx, n.cast())?)?) ;
    arm_t_windowclause => Ok(Node::mk_window_clause(mcx, conv_windowclause(mcx, n.cast())?)?) ;
    arm_t_sortgroupclause => {
            Ok(Node::mk_sort_group_clause(mcx, conv_sortgroupclause(n.cast()))?)
        } ;
    arm_t_rowmarkclause => Ok(Node::mk_row_mark_clause(mcx, conv_rowmark(n.cast()))?) ;
    arm_t_lockingclause => {
            Ok(Node::mk_locking_clause(mcx, conv_lockingclause(mcx, n.cast())?)?)
        } ;
    arm_t_integer => Ok(conv_value_node(mcx, n)?) ;
    arm_t_float => Ok(conv_value_node(mcx, n)?) ;
    arm_t_boolean => Ok(conv_value_node(mcx, n)?) ;
    arm_t_string => Ok(conv_value_node(mcx, n)?) ;
    arm_t_bitstring => Ok(conv_value_node(mcx, n)?) ;
    arm_t_boolexpr => Ok(Node::mk_bool_expr(mcx, conv_boolexpr(mcx, n.cast())?)?) ;
    arm_t_caseexpr => Ok(Node::mk_case_expr(mcx, conv_caseexpr(mcx, n.cast())?)?) ;
    arm_t_casewhen => Ok(Node::mk_case_when(mcx, conv_casewhen(mcx, n.cast())?)?) ;
    arm_t_coalesceexpr => Ok(Node::mk_coalesce_expr(mcx, conv_coalesceexpr(mcx, n.cast())?)?) ;
    arm_t_minmaxexpr => Ok(Node::mk_min_max_expr(mcx, conv_minmaxexpr(mcx, n.cast())?)?) ;
    arm_t_sublink => Ok(Node::mk_sub_link(mcx, conv_sublink(mcx, n.cast())?)?) ;
    arm_t_nulltest => Ok(Node::mk_null_test(mcx, conv_nulltest(mcx, n.cast())?)?) ;
    arm_t_booleantest => Ok(Node::mk_boolean_test(mcx, conv_booleantest(mcx, n.cast())?)?) ;
    arm_t_rowexpr => Ok(Node::mk_row_expr(mcx, conv_rowexpr(mcx, n.cast())?)?) ;
    arm_t_groupingfunc => Ok(Node::mk_grouping_func(mcx, conv_groupingfunc(mcx, n.cast())?)?) ;
    arm_t_collateexpr => Ok(Node::mk_collate_expr(mcx, conv_collateexpr(mcx, n.cast())?)?) ;
    arm_t_settodefault => Ok(Node::mk_set_to_default(mcx, conv_settodefault(n.cast()))?) ;
    arm_t_currentofexpr => {
            Ok(Node::mk_current_of_expr(mcx, conv_currentofexpr(mcx, n.cast())?)?)
        } ;
    arm_t_namedargexpr => Ok(Node::mk_named_arg_expr(mcx, conv_namedargexpr(mcx, n.cast())?)?) ;
    arm_t_mergesupportfunc => {
            Ok(Node::mk_merge_support_func(mcx, conv_mergesupportfunc(n.cast()))?)
        } ;
    arm_t_sqlvaluefunction => {
            Ok(Node::mk_sql_value_function(mcx, conv_sqlvaluefunction(n.cast()))?)
        } ;
    arm_t_xmlexpr => Ok(Node::mk_xml_expr(mcx, conv_xmlexpr(mcx, n.cast())?)?) ;
    arm_t_xmlserialize => Ok(Node::mk_xml_serialize(mcx, conv_xmlserialize(mcx, n.cast())?)?) ;
    arm_t_jsonvalueexpr => {
            Ok(Node::mk_json_value_expr(mcx, conv_json_value_expr(mcx, n.cast())?)?)
        } ;
    arm_t_jsonbehavior => {
            Ok(Node::mk_json_behavior(mcx, conv_json_behavior(mcx, n.cast())?)?)
        } ;
    arm_t_jsonispredicate => {
            Ok(Node::mk_json_is_predicate(mcx, conv_json_is_predicate(mcx, n.cast())?)?)
        } ;
    arm_t_jsonoutput => Ok(Node::mk_json_output(mcx, conv_json_output(mcx, n.cast())?)?) ;
    arm_t_jsonkeyvalue => {
            Ok(Node::mk_json_key_value(mcx, conv_json_key_value(mcx, n.cast())?)?)
        } ;
    arm_t_jsonargument => {
            Ok(Node::mk_json_argument(mcx, conv_json_argument(mcx, n.cast())?)?)
        } ;
    arm_t_jsonobjectconstructor => Ok(Node::mk_json_object_constructor(
            mcx,
            conv_json_object_constructor(mcx, n.cast())?,
        )?) ;
    arm_t_jsonarrayconstructor => Ok(Node::mk_json_array_constructor(
            mcx,
            conv_json_array_constructor(mcx, n.cast())?,
        )?) ;
    arm_t_jsonarrayqueryconstructor => Ok(Node::mk_json_array_query_constructor(
            mcx,
            conv_json_array_query_constructor(mcx, n.cast())?,
        )?) ;
    arm_t_jsonaggconstructor => Ok(Node::mk_json_agg_constructor(
            mcx,
            conv_json_agg_constructor(mcx, n.cast())?,
        )?) ;
    arm_t_jsonobjectagg => {
            Ok(Node::mk_json_object_agg(mcx, conv_json_object_agg(mcx, n.cast())?)?)
        } ;
    arm_t_jsonarrayagg => {
            Ok(Node::mk_json_array_agg(mcx, conv_json_array_agg(mcx, n.cast())?)?)
        } ;
    arm_t_jsonparseexpr => {
            Ok(Node::mk_json_parse_expr(mcx, conv_json_parse_expr(mcx, n.cast())?)?)
        } ;
    arm_t_jsonscalarexpr => {
            Ok(Node::mk_json_scalar_expr(mcx, conv_json_scalar_expr(mcx, n.cast())?)?)
        } ;
    arm_t_jsonserializeexpr => Ok(Node::mk_json_serialize_expr(
            mcx,
            conv_json_serialize_expr(mcx, n.cast())?,
        )?) ;
    arm_t_jsonfuncexpr => {
            Ok(Node::mk_json_func_expr(mcx, conv_json_func_expr(mcx, n.cast())?)?)
        } ;
    arm_t_jsontablepathspec => Ok(Node::mk_json_table_path_spec(
            mcx,
            conv_json_table_path_spec(mcx, n.cast())?,
        )?) ;
    arm_t_jsontable => Ok(Node::mk_json_table(mcx, conv_json_table(mcx, n.cast())?)?) ;
    arm_t_jsontablecolumn => {
            Ok(Node::mk_json_table_column(mcx, conv_json_table_column(mcx, n.cast())?)?)
        } ;
    arm_t_rolespec => Ok(Node::mk_role_spec(mcx, conv_rolespec(mcx, n.cast())?)?) ;
    arm_t_defelem => Ok(Node::mk_def_elem(mcx, conv_defelem(mcx, n.cast())?)?) ;
    arm_t_constraint => Ok(Node::mk_constraint(mcx, conv_constraint(mcx, n.cast())?)?) ;
    arm_t_tablelikeclause => {
            Ok(Node::mk_table_like_clause(mcx, conv_tablelikeclause(mcx, n.cast())?)?)
        } ;
    arm_t_indexelem => Ok(Node::mk_index_elem(mcx, conv_indexelem(mcx, n.cast())?)?) ;
    arm_t_functionparameter => {
            Ok(Node::mk_function_parameter(mcx, conv_functionparameter(mcx, n.cast())?)?)
        } ;
    arm_t_objectwithargs => {
            Ok(Node::mk_object_with_args(mcx, conv_objectwithargs(mcx, n.cast())?)?)
        } ;
    arm_t_accesspriv => Ok(Node::mk_access_priv(mcx, conv_accesspriv(mcx, n.cast())?)?) ;
    arm_t_createopclassitem => {
            Ok(Node::mk_create_op_class_item(mcx, conv_createopclassitem(mcx, n.cast())?)?)
        } ;
    arm_t_statselem => Ok(Node::mk_stats_elem(mcx, conv_statselem(mcx, n.cast())?)?) ;
    arm_t_partitionelem => {
            Ok(Node::mk_partition_elem(mcx, conv_partitionelem(mcx, n.cast())?)?)
        } ;
    arm_t_partitionspec => {
            Ok(Node::mk_partition_spec(mcx, conv_partitionspec(mcx, n.cast())?)?)
        } ;
    arm_t_partitionboundspec => {
            Ok(Node::mk_partition_bound_spec(mcx, conv_partitionboundspec(mcx, n.cast())?)?)
        } ;
    arm_t_partitionrangedatum => {
            Ok(Node::mk_partition_range_datum(mcx, conv_partitionrangedatum(mcx, n.cast())?)?)
        } ;
    arm_t_intoclause => Ok(Node::mk_into_clause(mcx, conv_intoclause(mcx, n.cast())?)?) ;
    arm_t_createstmt => Ok(Node::mk_create_stmt(mcx, conv_createstmt(mcx, n.cast())?)?) ;
    arm_t_indexstmt => Ok(Node::mk_index_stmt(mcx, conv_indexstmt(mcx, n.cast())?)?) ;
    arm_t_createseqstmt => {
            Ok(Node::mk_create_seq_stmt(mcx, conv_createseqstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createstatsstmt => {
            Ok(Node::mk_create_stats_stmt(mcx, conv_createstatsstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createfunctionstmt => {
            Ok(Node::mk_create_function_stmt(mcx, conv_createfunctionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_definestmt => Ok(Node::mk_define_stmt(mcx, conv_definestmt(mcx, n.cast())?)?) ;
    arm_t_createdomainstmt => {
            Ok(Node::mk_create_domain_stmt(mcx, conv_createdomainstmt(mcx, n.cast())?)?)
        } ;
    arm_t_compositetypestmt => {
            Ok(Node::mk_composite_type_stmt(mcx, conv_compositetypestmt(mcx, n.cast())?)?)
        } ;
    arm_t_createenumstmt => {
            Ok(Node::mk_create_enum_stmt(mcx, conv_createenumstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createrangestmt => {
            Ok(Node::mk_create_range_stmt(mcx, conv_createrangestmt(mcx, n.cast())?)?)
        } ;
    arm_t_viewstmt => Ok(Node::mk_view_stmt(mcx, conv_viewstmt(mcx, n.cast())?)?) ;
    arm_t_createtableasstmt => {
            Ok(Node::mk_create_table_as_stmt(mcx, conv_createtableasstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createschemastmt => {
            Ok(Node::mk_create_schema_stmt(mcx, conv_createschemastmt(mcx, n.cast())?)?)
        } ;
    arm_t_createextensionstmt => {
            Ok(Node::mk_create_extension_stmt(mcx, conv_createextensionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createtrigstmt => {
            Ok(Node::mk_create_trig_stmt(mcx, conv_createtrigstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createrolestmt => {
            Ok(Node::mk_create_role_stmt(mcx, conv_createrolestmt(mcx, n.cast())?)?)
        } ;
    arm_t_createdbstmt => Ok(Node::mk_createdb_stmt(mcx, conv_createdbstmt(mcx, n.cast())?)?) ;
    arm_t_createcaststmt => {
            Ok(Node::mk_create_cast_stmt(mcx, conv_createcaststmt(mcx, n.cast())?)?)
        } ;
    arm_t_createopclassstmt => {
            Ok(Node::mk_create_op_class_stmt(mcx, conv_createopclassstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createopfamilystmt => {
            Ok(Node::mk_create_op_family_stmt(mcx, conv_createopfamilystmt(mcx, n.cast())?)?)
        } ;
    arm_t_createplangstmt => {
            Ok(Node::mk_create_p_lang_stmt(mcx, conv_createplangstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createtablespacestmt => {
            Ok(Node::mk_create_table_space_stmt(mcx, conv_createtablespacestmt(mcx, n.cast())?)?)
        } ;
    arm_t_createconversionstmt => {
            Ok(Node::mk_create_conversion_stmt(mcx, conv_createconversionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createamstmt => Ok(Node::mk_create_am_stmt(mcx, conv_createamstmt(mcx, n.cast())?)?) ;
    arm_t_partitioncmd => Ok(Node::mk_partition_cmd(mcx, conv_partitioncmd(mcx, n.cast())?)?) ;
    arm_t_replicaidentitystmt => {
            Ok(Node::mk_replica_identity_stmt(mcx, conv_replicaidentitystmt(mcx, n.cast())?)?)
        } ;
    arm_t_atalterconstraint => {
            Ok(Node::mk_at_alter_constraint(mcx, conv_ataltconstraint(mcx, n.cast())?)?)
        } ;
    arm_t_altertablestmt => {
            Ok(Node::mk_alter_table_stmt(mcx, conv_altertablestmt(mcx, n.cast())?)?)
        } ;
    arm_t_altertablecmd => Ok(Node::mk_alter_table_cmd(mcx, conv_altertablecmd(mcx, n.cast())?)?) ;
    arm_t_altercollationstmt => {
            Ok(Node::mk_alter_collation_stmt(mcx, conv_altercollationstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterdomainstmt => {
            Ok(Node::mk_alter_domain_stmt(mcx, conv_alterdomainstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterenumstmt => Ok(Node::mk_alter_enum_stmt(mcx, conv_alterenumstmt(mcx, n.cast())?)?) ;
    arm_t_alterstatsstmt => {
            Ok(Node::mk_alter_stats_stmt(mcx, conv_alterstatsstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterseqstmt => Ok(Node::mk_alter_seq_stmt(mcx, conv_alterseqstmt(mcx, n.cast())?)?) ;
    arm_t_alteropfamilystmt => {
            Ok(Node::mk_alter_op_family_stmt(mcx, conv_alteropfamilystmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterfunctionstmt => {
            Ok(Node::mk_alter_function_stmt(mcx, conv_alterfunctionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_dropstmt => Ok(Node::mk_drop_stmt(mcx, conv_dropstmt(mcx, n.cast())?)?) ;
    arm_t_renamestmt => Ok(Node::mk_rename_stmt(mcx, conv_renamestmt(mcx, n.cast())?)?) ;
    arm_t_alterobjectdependsstmt => {
            Ok(Node::mk_alter_object_depends_stmt(mcx, conv_alterobjectdependsstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterobjectschemastmt => {
            Ok(Node::mk_alter_object_schema_stmt(mcx, conv_alterobjectschemastmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterownerstmt => {
            Ok(Node::mk_alter_owner_stmt(mcx, conv_alterownerstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alteroperatorstmt => {
            Ok(Node::mk_alter_operator_stmt(mcx, conv_alteroperatorstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altertypestmt => Ok(Node::mk_alter_type_stmt(mcx, conv_altertypestmt(mcx, n.cast())?)?) ;
    arm_t_alterdefaultprivilegesstmt => Ok(Node::mk_alter_default_privileges_stmt(mcx, 
            conv_alterdefaultprivilegesstmt(mcx, n.cast())?,
        )?) ;
    arm_t_alterrolestmt => Ok(Node::mk_alter_role_stmt(mcx, conv_alterrolestmt(mcx, n.cast())?)?) ;
    arm_t_alterrolesetstmt => {
            Ok(Node::mk_alter_role_set_stmt(mcx, conv_alterrolesetstmt(mcx, n.cast())?)?)
        } ;
    arm_t_dropownedstmt => Ok(Node::mk_drop_owned_stmt(mcx, conv_dropownedstmt(mcx, n.cast())?)?) ;
    arm_t_reassignownedstmt => {
            Ok(Node::mk_reassign_owned_stmt(mcx, conv_reassignownedstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altertablespaceoptionsstmt => Ok(Node::mk_alter_table_space_options_stmt(mcx, 
            conv_altertablespaceoptionsstmt(mcx, n.cast())?,
        )?) ;
    arm_t_altertablemoveallstmt => {
            Ok(Node::mk_alter_table_move_all_stmt(mcx, conv_altertablemoveallstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterextensionstmt => {
            Ok(Node::mk_alter_extension_stmt(mcx, conv_alterextensionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterextensioncontentsstmt => Ok(Node::mk_alter_extension_contents_stmt(mcx, 
            conv_alterextensioncontentsstmt(mcx, n.cast())?,
        )?) ;
    arm_t_alterfdwstmt => Ok(Node::mk_alter_fdw_stmt(mcx, conv_alterfdwstmt(mcx, n.cast())?)?) ;
    arm_t_alterforeignserverstmt => {
            Ok(Node::mk_alter_foreign_server_stmt(mcx, conv_alterforeignserverstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterusermappingstmt => {
            Ok(Node::mk_alter_user_mapping_stmt(mcx, conv_alterusermappingstmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterpolicystmt => {
            Ok(Node::mk_alter_policy_stmt(mcx, conv_alterpolicystmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterdatabasestmt => {
            Ok(Node::mk_alter_database_stmt(mcx, conv_alterdatabasestmt(mcx, n.cast())?)?)
        } ;
    arm_t_alterdatabaserefreshcollstmt => Ok(Node::mk_alter_database_refresh_coll_stmt(mcx, 
            conv_alterdatabaserefreshcollstmt(mcx, n.cast())?,
        )?) ;
    arm_t_alterdatabasesetstmt => {
            Ok(Node::mk_alter_database_set_stmt(mcx, conv_alterdatabasesetstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altertsdictionarystmt => {
            Ok(Node::mk_alter_ts_dictionary_stmt(mcx, conv_altertsdictionarystmt(mcx, n.cast())?)?)
        } ;
    arm_t_altertsconfigurationstmt => Ok(Node::mk_alter_ts_configuration_stmt(mcx, 
            conv_altertsconfigurationstmt(mcx, n.cast())?,
        )?) ;
    arm_t_alterpublicationstmt => {
            Ok(Node::mk_alter_publication_stmt(mcx, conv_alterpublicationstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altersubscriptionstmt => {
            Ok(Node::mk_alter_subscription_stmt(mcx, conv_altersubscriptionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_grantstmt => Ok(Node::mk_grant_stmt(mcx, conv_grantstmt(mcx, n.cast())?)?) ;
    arm_t_grantrolestmt => Ok(Node::mk_grant_role_stmt(mcx, conv_grantrolestmt(mcx, n.cast())?)?) ;
    arm_t_variablesetstmt => Ok(Node::mk_variable_set_stmt(mcx, conv_variablesetstmt(mcx, n.cast())?)?) ;
    arm_t_variableshowstmt => {
            Ok(Node::mk_variable_show_stmt(mcx, conv_variableshowstmt(mcx, n.cast())?)?)
        } ;
    arm_t_transactionstmt => Ok(Node::mk_transaction_stmt(mcx, conv_transactionstmt(mcx, n.cast())?)?) ;
    arm_t_copystmt => Ok(Node::mk_copy_stmt(mcx, conv_copystmt(mcx, n.cast())?)?) ;
    arm_t_explainstmt => Ok(Node::mk_explain_stmt(mcx, conv_explainstmt(mcx, n.cast())?)?) ;
    arm_t_preparestmt => Ok(Node::mk_prepare_stmt(mcx, conv_preparestmt(mcx, n.cast())?)?) ;
    arm_t_executestmt => Ok(Node::mk_execute_stmt(mcx, conv_executestmt(mcx, n.cast())?)?) ;
    arm_t_deallocatestmt => Ok(Node::mk_deallocate_stmt(mcx, conv_deallocatestmt(mcx, n.cast())?)?) ;
    arm_t_declarecursorstmt => {
            Ok(Node::mk_declare_cursor_stmt(mcx, conv_declarecursorstmt(mcx, n.cast())?)?)
        } ;
    arm_t_closeportalstmt => Ok(Node::mk_close_portal_stmt(mcx, conv_closeportalstmt(mcx, n.cast())?)?) ;
    arm_t_fetchstmt => Ok(Node::mk_fetch_stmt(mcx, conv_fetchstmt(mcx, n.cast())?)?) ;
    arm_t_vacuumstmt => Ok(Node::mk_vacuum_stmt(mcx, conv_vacuumstmt(mcx, n.cast())?)?) ;
    arm_t_vacuumrelation => Ok(Node::mk_vacuum_relation(mcx, conv_vacuumrelation(mcx, n.cast())?)?) ;
    arm_t_clusterstmt => Ok(Node::mk_cluster_stmt(mcx, conv_clusterstmt(mcx, n.cast())?)?) ;
    arm_t_reindexstmt => Ok(Node::mk_reindex_stmt(mcx, conv_reindexstmt(mcx, n.cast())?)?) ;
    arm_t_checkpointstmt => Ok(Node::mk_check_point_stmt(mcx, tdn::CheckPointStmt)?) ;
    arm_t_discardstmt => Ok(Node::mk_discard_stmt(mcx, conv_discardstmt(n.cast()))?) ;
    arm_t_lockstmt => Ok(Node::mk_lock_stmt(mcx, conv_lockstmt(mcx, n.cast())?)?) ;
    arm_t_constraintssetstmt => {
            Ok(Node::mk_constraints_set_stmt(mcx, conv_constraintssetstmt(mcx, n.cast())?)?)
        } ;
    arm_t_loadstmt => Ok(Node::mk_load_stmt(mcx, conv_loadstmt(mcx, n.cast())?)?) ;
    arm_t_truncatestmt => Ok(Node::mk_truncate_stmt(mcx, conv_truncatestmt(mcx, n.cast())?)?) ;
    arm_t_commentstmt => Ok(Node::mk_comment_stmt(mcx, conv_commentstmt(mcx, n.cast())?)?) ;
    arm_t_seclabelstmt => Ok(Node::mk_sec_label_stmt(mcx, conv_seclabelstmt(mcx, n.cast())?)?) ;
    arm_t_rulestmt => Ok(Node::mk_rule_stmt(mcx, conv_rulestmt(mcx, n.cast())?)?) ;
    arm_t_notifystmt => Ok(Node::mk_notify_stmt(mcx, conv_notifystmt(mcx, n.cast())?)?) ;
    arm_t_listenstmt => Ok(Node::mk_listen_stmt(mcx, conv_listenstmt(mcx, n.cast())?)?) ;
    arm_t_unlistenstmt => Ok(Node::mk_unlisten_stmt(mcx, conv_unlistenstmt(mcx, n.cast())?)?) ;
    arm_t_dostmt => Ok(Node::mk_do_stmt(mcx, conv_dostmt(mcx, n.cast())?)?) ;
    arm_t_callstmt => Ok(Node::mk_call_stmt(mcx, conv_callstmt(mcx, n.cast())?)?) ;
    arm_t_refreshmatviewstmt => {
            Ok(Node::mk_refresh_mat_view_stmt(mcx, conv_refreshmatviewstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altersystemstmt => Ok(Node::mk_alter_system_stmt(mcx, conv_altersystemstmt(mcx, n.cast())?)?) ;
    arm_t_dropdbstmt => Ok(Node::mk_dropdb_stmt(mcx, conv_dropdbstmt(mcx, n.cast())?)?) ;
    arm_t_droprolestmt => Ok(Node::mk_drop_role_stmt(mcx, conv_droprolestmt(mcx, n.cast())?)?) ;
    arm_t_droptablespacestmt => {
            Ok(Node::mk_drop_table_space_stmt(mcx, conv_droptablespacestmt(mcx, n.cast())?)?)
        } ;
    arm_t_createfdwstmt => Ok(Node::mk_create_fdw_stmt(mcx, conv_createfdwstmt(mcx, n.cast())?)?) ;
    arm_t_createforeignserverstmt => Ok(Node::mk_create_foreign_server_stmt(mcx, 
            conv_createforeignserverstmt(mcx, n.cast())?,
        )?) ;
    arm_t_createforeigntablestmt => Ok(Node::mk_create_foreign_table_stmt(mcx, 
            conv_createforeigntablestmt(mcx, n.cast())?,
        )?) ;
    arm_t_createusermappingstmt => {
            Ok(Node::mk_create_user_mapping_stmt(mcx, conv_createusermappingstmt(mcx, n.cast())?)?)
        } ;
    arm_t_dropusermappingstmt => {
            Ok(Node::mk_drop_user_mapping_stmt(mcx, conv_dropusermappingstmt(mcx, n.cast())?)?)
        } ;
    arm_t_importforeignschemastmt => Ok(Node::mk_import_foreign_schema_stmt(mcx, 
            conv_importforeignschemastmt(mcx, n.cast())?,
        )?) ;
    arm_t_createpolicystmt => Ok(Node::mk_create_policy_stmt(mcx, conv_createpolicystmt(mcx, n.cast())?)?) ;
    arm_t_publicationtable => Ok(Node::mk_publication_table(mcx, conv_publicationtable(mcx, n.cast())?)?) ;
    arm_t_publicationobjspec => {
            Ok(Node::mk_publication_obj_spec(mcx, conv_publicationobjspec(mcx, n.cast())?)?)
        } ;
    arm_t_createpublicationstmt => {
            Ok(Node::mk_create_publication_stmt(mcx, conv_createpublicationstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createsubscriptionstmt => Ok(Node::mk_create_subscription_stmt(mcx, 
            conv_createsubscriptionstmt(mcx, n.cast())?,
        )?) ;
    arm_t_dropsubscriptionstmt => {
            Ok(Node::mk_drop_subscription_stmt(mcx, conv_dropsubscriptionstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createeventtrigstmt => {
            Ok(Node::mk_create_event_trig_stmt(mcx, conv_createeventtrigstmt(mcx, n.cast())?)?)
        } ;
    arm_t_altereventtrigstmt => {
            Ok(Node::mk_alter_event_trig_stmt(mcx, conv_altereventtrigstmt(mcx, n.cast())?)?)
        } ;
    arm_t_createtransformstmt => {
            Ok(Node::mk_create_transform_stmt(mcx, conv_createtransformstmt(mcx, n.cast())?)?)
        } ;
    arm_t_returnstmt => Ok(Node::mk_return_stmt(mcx, conv_returnstmt(mcx, n.cast())?)?) ;
    arm_t_plassignstmt => Ok(Node::mk_pl_assign_stmt(mcx, conv_plassignstmt(mcx, n.cast())?)?) ;
}

/// Convert any `*mut Node` (dispatch on the leading `type_` tag).
pub fn convert_node<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Node<'mcx>> {
    let tag = unsafe { (*n).type_ };
    match tag {
        tags::T_List => arm_t_list(mcx, n),
        tags::T_SelectStmt => arm_t_selectstmt(mcx, n),
        tags::T_InsertStmt => arm_t_insertstmt(mcx, n),
        tags::T_UpdateStmt => arm_t_updatestmt(mcx, n),
        tags::T_DeleteStmt => arm_t_deletestmt(mcx, n),
        tags::T_MergeStmt => arm_t_mergestmt(mcx, n),
        tags::T_SetOperationStmt => arm_t_setoperationstmt(mcx, n),
        tags::T_A_Expr => arm_t_a_expr(mcx, n),
        tags::T_ColumnRef => arm_t_columnref(mcx, n),
        tags::T_ParamRef => arm_t_paramref(mcx, n),
        tags::T_A_Const => arm_t_a_const(mcx, n),
        tags::T_FuncCall => arm_t_funccall(mcx, n),
        tags::T_A_Star => arm_t_a_star(mcx, n),
        tags::T_A_Indices => arm_t_a_indices(mcx, n),
        tags::T_A_Indirection => arm_t_a_indirection(mcx, n),
        tags::T_A_ArrayExpr => arm_t_a_arrayexpr(mcx, n),
        tags::T_ResTarget => arm_t_restarget(mcx, n),
        tags::T_MultiAssignRef => arm_t_multiassignref(mcx, n),
        tags::T_TypeCast => arm_t_typecast(mcx, n),
        tags::T_CollateClause => arm_t_collateclause(mcx, n),
        tags::T_SortBy => arm_t_sortby(mcx, n),
        tags::T_WindowDef => arm_t_windowdef(mcx, n),
        tags::T_RangeSubselect => arm_t_rangesubselect(mcx, n),
        tags::T_RangeFunction => arm_t_rangefunction(mcx, n),
        tags::T_RangeTableSample => arm_t_rangetablesample(mcx, n),
        tags::T_TypeName => arm_t_typename(mcx, n),
        tags::T_ColumnDef => arm_t_columndef(mcx, n),
        tags::T_RangeVar => arm_t_rangevar(mcx, n),
        tags::T_JoinExpr => arm_t_joinexpr(mcx, n),
        tags::T_FromExpr => arm_t_fromexpr(mcx, n),
        tags::T_RangeTblRef => arm_t_rangetblref(mcx, n),
        tags::T_Alias => arm_t_alias(mcx, n),
        tags::T_WithClause => arm_t_withclause(mcx, n),
        tags::T_CommonTableExpr => arm_t_commontableexpr(mcx, n),
        tags::T_InferClause => arm_t_inferclause(mcx, n),
        tags::T_OnConflictClause => arm_t_onconflictclause(mcx, n),
        tags::T_MergeWhenClause => arm_t_mergewhenclause(mcx, n),
        tags::T_ReturningClause => arm_t_returningclause(mcx, n),
        tags::T_ReturningOption => arm_t_returningoption(mcx, n),
        tags::T_TriggerTransition => arm_t_triggertransition(mcx, n),
        tags::T_RangeTableFunc => arm_t_rangetablefunc(mcx, n),
        tags::T_RangeTableFuncCol => arm_t_rangetablefunccol(mcx, n),
        tags::T_GroupingSet => arm_t_groupingset(mcx, n),
        tags::T_WindowClause => arm_t_windowclause(mcx, n),
        tags::T_SortGroupClause => arm_t_sortgroupclause(mcx, n),
        tags::T_RowMarkClause => arm_t_rowmarkclause(mcx, n),
        tags::T_LockingClause => arm_t_lockingclause(mcx, n),
        tags::T_Integer => arm_t_integer(mcx, n),
        tags::T_Float => arm_t_float(mcx, n),
        tags::T_Boolean => arm_t_boolean(mcx, n),
        tags::T_String => arm_t_string(mcx, n),
        tags::T_BitString => arm_t_bitstring(mcx, n),
        tags::T_BoolExpr => arm_t_boolexpr(mcx, n),
        tags::T_CaseExpr => arm_t_caseexpr(mcx, n),
        tags::T_CaseWhen => arm_t_casewhen(mcx, n),
        tags::T_CoalesceExpr => arm_t_coalesceexpr(mcx, n),
        tags::T_MinMaxExpr => arm_t_minmaxexpr(mcx, n),
        tags::T_SubLink => arm_t_sublink(mcx, n),
        tags::T_NullTest => arm_t_nulltest(mcx, n),
        tags::T_BooleanTest => arm_t_booleantest(mcx, n),
        tags::T_RowExpr => arm_t_rowexpr(mcx, n),
        tags::T_GroupingFunc => arm_t_groupingfunc(mcx, n),
        tags::T_CollateExpr => arm_t_collateexpr(mcx, n),
        tags::T_SetToDefault => arm_t_settodefault(mcx, n),
        tags::T_CurrentOfExpr => arm_t_currentofexpr(mcx, n),
        tags::T_NamedArgExpr => arm_t_namedargexpr(mcx, n),
        tags::T_MergeSupportFunc => arm_t_mergesupportfunc(mcx, n),
        tags::T_SQLValueFunction => arm_t_sqlvaluefunction(mcx, n),
        tags::T_XmlExpr => arm_t_xmlexpr(mcx, n),
        tags::T_XmlSerialize => arm_t_xmlserialize(mcx, n),
        tags::T_JsonValueExpr => arm_t_jsonvalueexpr(mcx, n),
        tags::T_JsonBehavior => arm_t_jsonbehavior(mcx, n),
        tags::T_JsonIsPredicate => arm_t_jsonispredicate(mcx, n),
        tags::T_JsonOutput => arm_t_jsonoutput(mcx, n),
        tags::T_JsonKeyValue => arm_t_jsonkeyvalue(mcx, n),
        tags::T_JsonArgument => arm_t_jsonargument(mcx, n),
        tags::T_JsonObjectConstructor => arm_t_jsonobjectconstructor(mcx, n),
        tags::T_JsonArrayConstructor => arm_t_jsonarrayconstructor(mcx, n),
        tags::T_JsonArrayQueryConstructor => arm_t_jsonarrayqueryconstructor(mcx, n),
        tags::T_JsonAggConstructor => arm_t_jsonaggconstructor(mcx, n),
        tags::T_JsonObjectAgg => arm_t_jsonobjectagg(mcx, n),
        tags::T_JsonArrayAgg => arm_t_jsonarrayagg(mcx, n),
        tags::T_JsonParseExpr => arm_t_jsonparseexpr(mcx, n),
        tags::T_JsonScalarExpr => arm_t_jsonscalarexpr(mcx, n),
        tags::T_JsonSerializeExpr => arm_t_jsonserializeexpr(mcx, n),
        tags::T_JsonFuncExpr => arm_t_jsonfuncexpr(mcx, n),
        tags::T_JsonTablePathSpec => arm_t_jsontablepathspec(mcx, n),
        tags::T_JsonTable => arm_t_jsontable(mcx, n),
        tags::T_JsonTableColumn => arm_t_jsontablecolumn(mcx, n),
        tags::T_RoleSpec => arm_t_rolespec(mcx, n),
        tags::T_DefElem => arm_t_defelem(mcx, n),
        tags::T_Constraint => arm_t_constraint(mcx, n),
        tags::T_TableLikeClause => arm_t_tablelikeclause(mcx, n),
        tags::T_IndexElem => arm_t_indexelem(mcx, n),
        tags::T_FunctionParameter => arm_t_functionparameter(mcx, n),
        tags::T_ObjectWithArgs => arm_t_objectwithargs(mcx, n),
        tags::T_AccessPriv => arm_t_accesspriv(mcx, n),
        tags::T_CreateOpClassItem => arm_t_createopclassitem(mcx, n),
        tags::T_StatsElem => arm_t_statselem(mcx, n),
        tags::T_PartitionElem => arm_t_partitionelem(mcx, n),
        tags::T_PartitionSpec => arm_t_partitionspec(mcx, n),
        tags::T_PartitionBoundSpec => arm_t_partitionboundspec(mcx, n),
        tags::T_PartitionRangeDatum => arm_t_partitionrangedatum(mcx, n),
        tags::T_IntoClause => arm_t_intoclause(mcx, n),
        tags::T_CreateStmt => arm_t_createstmt(mcx, n),
        tags::T_IndexStmt => arm_t_indexstmt(mcx, n),
        tags::T_CreateSeqStmt => arm_t_createseqstmt(mcx, n),
        tags::T_CreateStatsStmt => arm_t_createstatsstmt(mcx, n),
        tags::T_CreateFunctionStmt => arm_t_createfunctionstmt(mcx, n),
        tags::T_DefineStmt => arm_t_definestmt(mcx, n),
        tags::T_CreateDomainStmt => arm_t_createdomainstmt(mcx, n),
        tags::T_CompositeTypeStmt => arm_t_compositetypestmt(mcx, n),
        tags::T_CreateEnumStmt => arm_t_createenumstmt(mcx, n),
        tags::T_CreateRangeStmt => arm_t_createrangestmt(mcx, n),
        tags::T_ViewStmt => arm_t_viewstmt(mcx, n),
        tags::T_CreateTableAsStmt => arm_t_createtableasstmt(mcx, n),
        tags::T_CreateSchemaStmt => arm_t_createschemastmt(mcx, n),
        tags::T_CreateExtensionStmt => arm_t_createextensionstmt(mcx, n),
        tags::T_CreateTrigStmt => arm_t_createtrigstmt(mcx, n),
        tags::T_CreateRoleStmt => arm_t_createrolestmt(mcx, n),
        tags::T_CreatedbStmt => arm_t_createdbstmt(mcx, n),
        tags::T_CreateCastStmt => arm_t_createcaststmt(mcx, n),
        tags::T_CreateOpClassStmt => arm_t_createopclassstmt(mcx, n),
        tags::T_CreateOpFamilyStmt => arm_t_createopfamilystmt(mcx, n),
        tags::T_CreatePLangStmt => arm_t_createplangstmt(mcx, n),
        tags::T_CreateTableSpaceStmt => arm_t_createtablespacestmt(mcx, n),
        tags::T_CreateConversionStmt => arm_t_createconversionstmt(mcx, n),
        tags::T_CreateAmStmt => arm_t_createamstmt(mcx, n),
        tags::T_PartitionCmd => arm_t_partitioncmd(mcx, n),
        tags::T_ReplicaIdentityStmt => arm_t_replicaidentitystmt(mcx, n),
        tags::T_ATAlterConstraint => arm_t_atalterconstraint(mcx, n),
        tags::T_AlterTableStmt => arm_t_altertablestmt(mcx, n),
        tags::T_AlterTableCmd => arm_t_altertablecmd(mcx, n),
        tags::T_AlterCollationStmt => arm_t_altercollationstmt(mcx, n),
        tags::T_AlterDomainStmt => arm_t_alterdomainstmt(mcx, n),
        tags::T_AlterEnumStmt => arm_t_alterenumstmt(mcx, n),
        tags::T_AlterStatsStmt => arm_t_alterstatsstmt(mcx, n),
        tags::T_AlterSeqStmt => arm_t_alterseqstmt(mcx, n),
        tags::T_AlterOpFamilyStmt => arm_t_alteropfamilystmt(mcx, n),
        tags::T_AlterFunctionStmt => arm_t_alterfunctionstmt(mcx, n),
        tags::T_DropStmt => arm_t_dropstmt(mcx, n),
        tags::T_RenameStmt => arm_t_renamestmt(mcx, n),
        tags::T_AlterObjectDependsStmt => arm_t_alterobjectdependsstmt(mcx, n),
        tags::T_AlterObjectSchemaStmt => arm_t_alterobjectschemastmt(mcx, n),
        tags::T_AlterOwnerStmt => arm_t_alterownerstmt(mcx, n),
        tags::T_AlterOperatorStmt => arm_t_alteroperatorstmt(mcx, n),
        tags::T_AlterTypeStmt => arm_t_altertypestmt(mcx, n),
        tags::T_AlterDefaultPrivilegesStmt => arm_t_alterdefaultprivilegesstmt(mcx, n),
        tags::T_AlterRoleStmt => arm_t_alterrolestmt(mcx, n),
        tags::T_AlterRoleSetStmt => arm_t_alterrolesetstmt(mcx, n),
        tags::T_DropOwnedStmt => arm_t_dropownedstmt(mcx, n),
        tags::T_ReassignOwnedStmt => arm_t_reassignownedstmt(mcx, n),
        tags::T_AlterTableSpaceOptionsStmt => arm_t_altertablespaceoptionsstmt(mcx, n),
        tags::T_AlterTableMoveAllStmt => arm_t_altertablemoveallstmt(mcx, n),
        tags::T_AlterExtensionStmt => arm_t_alterextensionstmt(mcx, n),
        tags::T_AlterExtensionContentsStmt => arm_t_alterextensioncontentsstmt(mcx, n),
        tags::T_AlterFdwStmt => arm_t_alterfdwstmt(mcx, n),
        tags::T_AlterForeignServerStmt => arm_t_alterforeignserverstmt(mcx, n),
        tags::T_AlterUserMappingStmt => arm_t_alterusermappingstmt(mcx, n),
        tags::T_AlterPolicyStmt => arm_t_alterpolicystmt(mcx, n),
        tags::T_AlterDatabaseStmt => arm_t_alterdatabasestmt(mcx, n),
        tags::T_AlterDatabaseRefreshCollStmt => arm_t_alterdatabaserefreshcollstmt(mcx, n),
        tags::T_AlterDatabaseSetStmt => arm_t_alterdatabasesetstmt(mcx, n),
        tags::T_AlterTSDictionaryStmt => arm_t_altertsdictionarystmt(mcx, n),
        tags::T_AlterTSConfigurationStmt => arm_t_altertsconfigurationstmt(mcx, n),
        tags::T_AlterPublicationStmt => arm_t_alterpublicationstmt(mcx, n),
        tags::T_AlterSubscriptionStmt => arm_t_altersubscriptionstmt(mcx, n),
        tags::T_GrantStmt => arm_t_grantstmt(mcx, n),
        tags::T_GrantRoleStmt => arm_t_grantrolestmt(mcx, n),
        tags::T_VariableSetStmt => arm_t_variablesetstmt(mcx, n),
        tags::T_VariableShowStmt => arm_t_variableshowstmt(mcx, n),
        tags::T_TransactionStmt => arm_t_transactionstmt(mcx, n),
        tags::T_CopyStmt => arm_t_copystmt(mcx, n),
        tags::T_ExplainStmt => arm_t_explainstmt(mcx, n),
        tags::T_PrepareStmt => arm_t_preparestmt(mcx, n),
        tags::T_ExecuteStmt => arm_t_executestmt(mcx, n),
        tags::T_DeallocateStmt => arm_t_deallocatestmt(mcx, n),
        tags::T_DeclareCursorStmt => arm_t_declarecursorstmt(mcx, n),
        tags::T_ClosePortalStmt => arm_t_closeportalstmt(mcx, n),
        tags::T_FetchStmt => arm_t_fetchstmt(mcx, n),
        tags::T_VacuumStmt => arm_t_vacuumstmt(mcx, n),
        tags::T_VacuumRelation => arm_t_vacuumrelation(mcx, n),
        tags::T_ClusterStmt => arm_t_clusterstmt(mcx, n),
        tags::T_ReindexStmt => arm_t_reindexstmt(mcx, n),
        tags::T_CheckPointStmt => arm_t_checkpointstmt(mcx, n),
        tags::T_DiscardStmt => arm_t_discardstmt(mcx, n),
        tags::T_LockStmt => arm_t_lockstmt(mcx, n),
        tags::T_ConstraintsSetStmt => arm_t_constraintssetstmt(mcx, n),
        tags::T_LoadStmt => arm_t_loadstmt(mcx, n),
        tags::T_TruncateStmt => arm_t_truncatestmt(mcx, n),
        tags::T_CommentStmt => arm_t_commentstmt(mcx, n),
        tags::T_SecLabelStmt => arm_t_seclabelstmt(mcx, n),
        tags::T_RuleStmt => arm_t_rulestmt(mcx, n),
        tags::T_NotifyStmt => arm_t_notifystmt(mcx, n),
        tags::T_ListenStmt => arm_t_listenstmt(mcx, n),
        tags::T_UnlistenStmt => arm_t_unlistenstmt(mcx, n),
        tags::T_DoStmt => arm_t_dostmt(mcx, n),
        tags::T_CallStmt => arm_t_callstmt(mcx, n),
        tags::T_RefreshMatViewStmt => arm_t_refreshmatviewstmt(mcx, n),
        tags::T_AlterSystemStmt => arm_t_altersystemstmt(mcx, n),
        tags::T_DropdbStmt => arm_t_dropdbstmt(mcx, n),
        tags::T_DropRoleStmt => arm_t_droprolestmt(mcx, n),
        tags::T_DropTableSpaceStmt => arm_t_droptablespacestmt(mcx, n),
        tags::T_CreateFdwStmt => arm_t_createfdwstmt(mcx, n),
        tags::T_CreateForeignServerStmt => arm_t_createforeignserverstmt(mcx, n),
        tags::T_CreateForeignTableStmt => arm_t_createforeigntablestmt(mcx, n),
        tags::T_CreateUserMappingStmt => arm_t_createusermappingstmt(mcx, n),
        tags::T_DropUserMappingStmt => arm_t_dropusermappingstmt(mcx, n),
        tags::T_ImportForeignSchemaStmt => arm_t_importforeignschemastmt(mcx, n),
        tags::T_CreatePolicyStmt => arm_t_createpolicystmt(mcx, n),
        tags::T_PublicationTable => arm_t_publicationtable(mcx, n),
        tags::T_PublicationObjSpec => arm_t_publicationobjspec(mcx, n),
        tags::T_CreatePublicationStmt => arm_t_createpublicationstmt(mcx, n),
        tags::T_CreateSubscriptionStmt => arm_t_createsubscriptionstmt(mcx, n),
        tags::T_DropSubscriptionStmt => arm_t_dropsubscriptionstmt(mcx, n),
        tags::T_CreateEventTrigStmt => arm_t_createeventtrigstmt(mcx, n),
        tags::T_AlterEventTrigStmt => arm_t_altereventtrigstmt(mcx, n),
        tags::T_CreateTransformStmt => arm_t_createtransformstmt(mcx, n),
        tags::T_ReturnStmt => arm_t_returnstmt(mcx, n),
        tags::T_PLAssignStmt => arm_t_plassignstmt(mcx, n),
        other => unported(other, node_tag_name(other)),
    }
}

include!("convert_stmts.rs");
include!("convert_exprs.rs");
include!("convert_misc.rs");
include!("convert_ddl.rs");
include!("convert_json.rs");
