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
use types_nodes::primnodes as tn_prim;
use types_nodes::value as tn_val;

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

/// Convert a `*mut RawStmt` (the raw list element) into the owned [`RawStmt`].
pub fn convert_raw_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    rs: *mut cs::RawStmt,
) -> PgResult<RawStmt<'mcx>> {
    let rs = unsafe { &*rs };
    Ok(RawStmt {
        stmt: node_req(mcx, rs.stmt)?,
        stmt_location: rs.stmt_location,
        stmt_len: rs.stmt_len,
    })
}

/// Convert any `*mut Node` (dispatch on the leading `type_` tag).
pub fn convert_node<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Node<'mcx>> {
    let tag = unsafe { (*n).type_ };
    match tag {
        // A bare `List *` node (e.g. a VALUES row): convert its elements.
        tags::T_List => {
            let l = n.cast::<RawList>();
            Ok(Node::List(node_list(mcx, l)?))
        }

        // --- statements ---
        tags::T_SelectStmt => Ok(Node::SelectStmt(conv_select(mcx, n.cast())?)),
        tags::T_InsertStmt => Ok(Node::InsertStmt(conv_insert(mcx, n.cast())?)),
        tags::T_UpdateStmt => Ok(Node::UpdateStmt(conv_update(mcx, n.cast())?)),
        tags::T_DeleteStmt => Ok(Node::DeleteStmt(conv_delete(mcx, n.cast())?)),
        tags::T_MergeStmt => Ok(Node::MergeStmt(conv_merge(mcx, n.cast())?)),
        tags::T_SetOperationStmt => {
            Ok(Node::SetOperationStmt(conv_setop_stmt(mcx, n.cast())?))
        }

        // --- raw expression / grammar nodes ---
        tags::T_A_Expr => Ok(Node::A_Expr(conv_a_expr(mcx, n.cast())?)),
        tags::T_ColumnRef => Ok(Node::ColumnRef(conv_columnref(mcx, n.cast())?)),
        tags::T_ParamRef => Ok(Node::ParamRef(conv_paramref(n.cast()))),
        tags::T_A_Const => Ok(conv_a_const(mcx, n.cast())?),
        tags::T_FuncCall => Ok(Node::FuncCall(conv_funccall(mcx, n.cast())?)),
        tags::T_A_Star => Ok(Node::A_Star(tn::A_Star)),
        tags::T_A_Indices => Ok(Node::A_Indices(conv_a_indices(mcx, n.cast())?)),
        tags::T_A_Indirection => {
            Ok(Node::A_Indirection(conv_a_indirection(mcx, n.cast())?))
        }
        tags::T_A_ArrayExpr => Ok(Node::A_ArrayExpr(conv_a_arrayexpr(mcx, n.cast())?)),
        tags::T_ResTarget => Ok(Node::ResTarget(conv_restarget(mcx, n.cast())?)),
        tags::T_MultiAssignRef => {
            Ok(Node::MultiAssignRef(conv_multiassignref(mcx, n.cast())?))
        }
        tags::T_TypeCast => Ok(Node::TypeCast(conv_typecast(mcx, n.cast())?)),
        tags::T_CollateClause => Ok(Node::CollateClause(conv_collate(mcx, n.cast())?)),
        tags::T_SortBy => Ok(Node::SortBy(conv_sortby(mcx, n.cast())?)),
        tags::T_WindowDef => Ok(Node::WindowDef(conv_windowdef(mcx, n.cast())?)),
        tags::T_RangeSubselect => {
            Ok(Node::RangeSubselect(conv_rangesubselect(mcx, n.cast())?))
        }
        tags::T_RangeFunction => Ok(Node::RangeFunction(conv_rangefunction(mcx, n.cast())?)),
        tags::T_RangeTableSample => {
            Ok(Node::RangeTableSample(conv_rangetablesample(mcx, n.cast())?))
        }
        tags::T_TypeName => Ok(Node::TypeName(conv_typename(mcx, n.cast())?)),
        tags::T_ColumnDef => Ok(Node::ColumnDef(conv_columndef(mcx, n.cast())?)),

        // --- range/join structure ---
        tags::T_RangeVar => Ok(Node::RangeVar(conv_rangevar(mcx, n.cast())?)),
        tags::T_JoinExpr => Ok(Node::JoinExpr(conv_joinexpr(mcx, n.cast())?)),
        tags::T_FromExpr => Ok(Node::FromExpr(conv_fromexpr(mcx, n.cast())?)),
        tags::T_RangeTblRef => Ok(Node::RangeTblRef(conv_rangetblref(n.cast()))),
        tags::T_Alias => Ok(Node::Alias(conv_alias(mcx, n.cast())?)),

        // --- clauses / specs ---
        tags::T_WithClause => Ok(Node::WithClause(conv_withclause(mcx, n.cast())?)),
        tags::T_CommonTableExpr => {
            Ok(Node::CommonTableExpr(conv_cte(mcx, n.cast())?))
        }
        tags::T_InferClause => Ok(Node::InferClause(conv_infer(mcx, n.cast())?)),
        tags::T_OnConflictClause => {
            Ok(Node::OnConflictClause(conv_onconflict_clause(mcx, n.cast())?))
        }
        tags::T_MergeWhenClause => {
            Ok(Node::MergeWhenClause(conv_mergewhen(mcx, n.cast())?))
        }
        tags::T_ReturningClause => {
            Ok(Node::ReturningClause(conv_returning(mcx, n.cast())?))
        }
        tags::T_GroupingSet => Ok(Node::GroupingSet(conv_groupingset(mcx, n.cast())?)),
        tags::T_WindowClause => Ok(Node::WindowClause(conv_windowclause(mcx, n.cast())?)),
        tags::T_SortGroupClause => {
            Ok(Node::SortGroupClause(conv_sortgroupclause(n.cast())))
        }
        tags::T_RowMarkClause => Ok(Node::RowMarkClause(conv_rowmark(n.cast()))),
        tags::T_LockingClause => {
            // No concrete `LockingClause` struct exists in types-nodes yet;
            // the grammar carries it in SelectStmt.lockingClause as raw list
            // elements. Until the type is authored, surfacing one is F2+.
            unported(tag, "LockingClause")
        }

        // --- value (leaf literal) nodes ---
        tags::T_Integer => Ok(conv_value_node(mcx, n)?),
        tags::T_Float => Ok(conv_value_node(mcx, n)?),
        tags::T_Boolean => Ok(conv_value_node(mcx, n)?),
        tags::T_String => Ok(conv_value_node(mcx, n)?),
        tags::T_BitString => Ok(conv_value_node(mcx, n)?),

        // --- grammar-produced Expr leaves ---
        tags::T_CaseExpr
        | tags::T_CoalesceExpr
        | tags::T_MinMaxExpr
        | tags::T_SubLink
        | tags::T_BooleanTest
        | tags::T_NullTest
        | tags::T_XmlExpr
        | tags::T_RowExpr
        | tags::T_GroupingFunc
        | tags::T_CollateExpr
        | tags::T_SetToDefault
        | tags::T_CurrentOfExpr
        | tags::T_NamedArgExpr
        | tags::T_BoolExpr
        | tags::T_SQLValueFunction => Ok(Node::Expr(conv_expr(mcx, n, tag)?)),

        // --- anything else: the absent DDL/utility node families (F2+) ---
        other => unported(other, node_tag_name(other)),
    }
}

include!("convert_stmts.rs");
include!("convert_exprs.rs");
include!("convert_misc.rs");
