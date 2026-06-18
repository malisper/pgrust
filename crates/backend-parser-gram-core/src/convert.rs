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
            None => out.push(mcx::alloc_in(mcx, Node::List(PgVec::new_in(mcx)))?),
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
            Ok(Node::LockingClause(conv_lockingclause(mcx, n.cast())?))
        }

        // --- value (leaf literal) nodes ---
        tags::T_Integer => Ok(conv_value_node(mcx, n)?),
        tags::T_Float => Ok(conv_value_node(mcx, n)?),
        tags::T_Boolean => Ok(conv_value_node(mcx, n)?),
        tags::T_String => Ok(conv_value_node(mcx, n)?),
        tags::T_BitString => Ok(conv_value_node(mcx, n)?),

        // --- grammar-produced raw `Expr`-deriving nodes (rawexprnodes) ---
        tags::T_BoolExpr => Ok(Node::BoolExpr(conv_boolexpr(mcx, n.cast())?)),
        tags::T_CaseExpr => Ok(Node::CaseExpr(conv_caseexpr(mcx, n.cast())?)),
        tags::T_CaseWhen => Ok(Node::CaseWhen(conv_casewhen(mcx, n.cast())?)),
        tags::T_CoalesceExpr => Ok(Node::CoalesceExpr(conv_coalesceexpr(mcx, n.cast())?)),
        tags::T_MinMaxExpr => Ok(Node::MinMaxExpr(conv_minmaxexpr(mcx, n.cast())?)),
        tags::T_SubLink => Ok(Node::SubLink(conv_sublink(mcx, n.cast())?)),
        tags::T_NullTest => Ok(Node::NullTest(conv_nulltest(mcx, n.cast())?)),
        tags::T_BooleanTest => Ok(Node::BooleanTest(conv_booleantest(mcx, n.cast())?)),
        tags::T_RowExpr => Ok(Node::RowExpr(conv_rowexpr(mcx, n.cast())?)),
        tags::T_GroupingFunc => Ok(Node::GroupingFunc(conv_groupingfunc(mcx, n.cast())?)),
        tags::T_CollateExpr => Ok(Node::CollateExpr(conv_collateexpr(mcx, n.cast())?)),
        tags::T_SetToDefault => Ok(Node::SetToDefault(conv_settodefault(n.cast()))),
        tags::T_CurrentOfExpr => {
            Ok(Node::CurrentOfExpr(conv_currentofexpr(mcx, n.cast())?))
        }
        tags::T_NamedArgExpr => Ok(Node::NamedArgExpr(conv_namedargexpr(mcx, n.cast())?)),
        tags::T_SQLValueFunction => {
            Ok(Node::SQLValueFunction(conv_sqlvaluefunction(n.cast())))
        }
        tags::T_XmlExpr => Ok(Node::XmlExpr(conv_xmlexpr(mcx, n.cast())?)),
        tags::T_XmlSerialize => Ok(Node::XmlSerialize(conv_xmlserialize(mcx, n.cast())?)),

        // --- DDL "CREATE" family (F2): supporting / helper nodes ---
        tags::T_RoleSpec => Ok(Node::RoleSpec(conv_rolespec(mcx, n.cast())?)),
        tags::T_DefElem => Ok(Node::DefElem(conv_defelem(mcx, n.cast())?)),
        tags::T_Constraint => Ok(Node::Constraint(conv_constraint(mcx, n.cast())?)),
        tags::T_TableLikeClause => {
            Ok(Node::TableLikeClause(conv_tablelikeclause(mcx, n.cast())?))
        }
        tags::T_IndexElem => Ok(Node::IndexElem(conv_indexelem(mcx, n.cast())?)),
        tags::T_FunctionParameter => {
            Ok(Node::FunctionParameter(conv_functionparameter(mcx, n.cast())?))
        }
        tags::T_ObjectWithArgs => {
            Ok(Node::ObjectWithArgs(conv_objectwithargs(mcx, n.cast())?))
        }
        tags::T_AccessPriv => Ok(Node::AccessPriv(conv_accesspriv(mcx, n.cast())?)),
        tags::T_CreateOpClassItem => {
            Ok(Node::CreateOpClassItem(conv_createopclassitem(mcx, n.cast())?))
        }
        tags::T_StatsElem => Ok(Node::StatsElem(conv_statselem(mcx, n.cast())?)),
        tags::T_PartitionElem => {
            Ok(Node::PartitionElem(conv_partitionelem(mcx, n.cast())?))
        }
        tags::T_PartitionSpec => {
            Ok(Node::PartitionSpec(conv_partitionspec(mcx, n.cast())?))
        }
        tags::T_PartitionBoundSpec => {
            Ok(Node::PartitionBoundSpec(conv_partitionboundspec(mcx, n.cast())?))
        }
        tags::T_PartitionRangeDatum => {
            Ok(Node::PartitionRangeDatum(conv_partitionrangedatum(mcx, n.cast())?))
        }
        tags::T_IntoClause => Ok(Node::IntoClause(conv_intoclause(mcx, n.cast())?)),

        // --- DDL "CREATE" family (F2): statements ---
        tags::T_CreateStmt => Ok(Node::CreateStmt(conv_createstmt(mcx, n.cast())?)),
        tags::T_IndexStmt => Ok(Node::IndexStmt(conv_indexstmt(mcx, n.cast())?)),
        tags::T_CreateSeqStmt => {
            Ok(Node::CreateSeqStmt(conv_createseqstmt(mcx, n.cast())?))
        }
        tags::T_CreateStatsStmt => {
            Ok(Node::CreateStatsStmt(conv_createstatsstmt(mcx, n.cast())?))
        }
        tags::T_CreateFunctionStmt => {
            Ok(Node::CreateFunctionStmt(conv_createfunctionstmt(mcx, n.cast())?))
        }
        tags::T_DefineStmt => Ok(Node::DefineStmt(conv_definestmt(mcx, n.cast())?)),
        tags::T_CreateDomainStmt => {
            Ok(Node::CreateDomainStmt(conv_createdomainstmt(mcx, n.cast())?))
        }
        tags::T_CompositeTypeStmt => {
            Ok(Node::CompositeTypeStmt(conv_compositetypestmt(mcx, n.cast())?))
        }
        tags::T_CreateEnumStmt => {
            Ok(Node::CreateEnumStmt(conv_createenumstmt(mcx, n.cast())?))
        }
        tags::T_CreateRangeStmt => {
            Ok(Node::CreateRangeStmt(conv_createrangestmt(mcx, n.cast())?))
        }
        tags::T_ViewStmt => Ok(Node::ViewStmt(conv_viewstmt(mcx, n.cast())?)),
        tags::T_CreateTableAsStmt => {
            Ok(Node::CreateTableAsStmt(conv_createtableasstmt(mcx, n.cast())?))
        }
        tags::T_CreateSchemaStmt => {
            Ok(Node::CreateSchemaStmt(conv_createschemastmt(mcx, n.cast())?))
        }
        tags::T_CreateExtensionStmt => {
            Ok(Node::CreateExtensionStmt(conv_createextensionstmt(mcx, n.cast())?))
        }
        tags::T_CreateTrigStmt => {
            Ok(Node::CreateTrigStmt(conv_createtrigstmt(mcx, n.cast())?))
        }
        tags::T_CreateRoleStmt => {
            Ok(Node::CreateRoleStmt(conv_createrolestmt(mcx, n.cast())?))
        }
        tags::T_CreatedbStmt => Ok(Node::CreatedbStmt(conv_createdbstmt(mcx, n.cast())?)),
        tags::T_CreateCastStmt => {
            Ok(Node::CreateCastStmt(conv_createcaststmt(mcx, n.cast())?))
        }
        tags::T_CreateOpClassStmt => {
            Ok(Node::CreateOpClassStmt(conv_createopclassstmt(mcx, n.cast())?))
        }
        tags::T_CreateOpFamilyStmt => {
            Ok(Node::CreateOpFamilyStmt(conv_createopfamilystmt(mcx, n.cast())?))
        }
        tags::T_CreatePLangStmt => {
            Ok(Node::CreatePLangStmt(conv_createplangstmt(mcx, n.cast())?))
        }
        tags::T_CreateTableSpaceStmt => {
            Ok(Node::CreateTableSpaceStmt(conv_createtablespacestmt(mcx, n.cast())?))
        }
        tags::T_CreateConversionStmt => {
            Ok(Node::CreateConversionStmt(conv_createconversionstmt(mcx, n.cast())?))
        }
        tags::T_CreateAmStmt => Ok(Node::CreateAmStmt(conv_createamstmt(mcx, n.cast())?)),

        // --- DDL "ALTER/DROP" family (F3): supporting / helper nodes ---
        tags::T_PartitionCmd => Ok(Node::PartitionCmd(conv_partitioncmd(mcx, n.cast())?)),
        tags::T_ReplicaIdentityStmt => {
            Ok(Node::ReplicaIdentityStmt(conv_replicaidentitystmt(mcx, n.cast())?))
        }
        tags::T_ATAlterConstraint => {
            Ok(Node::ATAlterConstraint(conv_ataltconstraint(mcx, n.cast())?))
        }

        // --- DDL "ALTER/DROP" family (F3): statements ---
        tags::T_AlterTableStmt => {
            Ok(Node::AlterTableStmt(conv_altertablestmt(mcx, n.cast())?))
        }
        tags::T_AlterTableCmd => Ok(Node::AlterTableCmd(conv_altertablecmd(mcx, n.cast())?)),
        tags::T_AlterCollationStmt => {
            Ok(Node::AlterCollationStmt(conv_altercollationstmt(mcx, n.cast())?))
        }
        tags::T_AlterDomainStmt => {
            Ok(Node::AlterDomainStmt(conv_alterdomainstmt(mcx, n.cast())?))
        }
        tags::T_AlterEnumStmt => Ok(Node::AlterEnumStmt(conv_alterenumstmt(mcx, n.cast())?)),
        tags::T_AlterStatsStmt => {
            Ok(Node::AlterStatsStmt(conv_alterstatsstmt(mcx, n.cast())?))
        }
        tags::T_AlterSeqStmt => Ok(Node::AlterSeqStmt(conv_alterseqstmt(mcx, n.cast())?)),
        tags::T_AlterOpFamilyStmt => {
            Ok(Node::AlterOpFamilyStmt(conv_alteropfamilystmt(mcx, n.cast())?))
        }
        tags::T_AlterFunctionStmt => {
            Ok(Node::AlterFunctionStmt(conv_alterfunctionstmt(mcx, n.cast())?))
        }
        tags::T_DropStmt => Ok(Node::DropStmt(conv_dropstmt(mcx, n.cast())?)),
        tags::T_RenameStmt => Ok(Node::RenameStmt(conv_renamestmt(mcx, n.cast())?)),
        tags::T_AlterObjectDependsStmt => {
            Ok(Node::AlterObjectDependsStmt(conv_alterobjectdependsstmt(mcx, n.cast())?))
        }
        tags::T_AlterObjectSchemaStmt => {
            Ok(Node::AlterObjectSchemaStmt(conv_alterobjectschemastmt(mcx, n.cast())?))
        }
        tags::T_AlterOwnerStmt => {
            Ok(Node::AlterOwnerStmt(conv_alterownerstmt(mcx, n.cast())?))
        }
        tags::T_AlterOperatorStmt => {
            Ok(Node::AlterOperatorStmt(conv_alteroperatorstmt(mcx, n.cast())?))
        }
        tags::T_AlterTypeStmt => Ok(Node::AlterTypeStmt(conv_altertypestmt(mcx, n.cast())?)),
        tags::T_AlterDefaultPrivilegesStmt => Ok(Node::AlterDefaultPrivilegesStmt(
            conv_alterdefaultprivilegesstmt(mcx, n.cast())?,
        )),
        tags::T_AlterRoleStmt => Ok(Node::AlterRoleStmt(conv_alterrolestmt(mcx, n.cast())?)),
        tags::T_AlterRoleSetStmt => {
            Ok(Node::AlterRoleSetStmt(conv_alterrolesetstmt(mcx, n.cast())?))
        }
        tags::T_DropOwnedStmt => Ok(Node::DropOwnedStmt(conv_dropownedstmt(mcx, n.cast())?)),
        tags::T_ReassignOwnedStmt => {
            Ok(Node::ReassignOwnedStmt(conv_reassignownedstmt(mcx, n.cast())?))
        }
        tags::T_AlterTableSpaceOptionsStmt => Ok(Node::AlterTableSpaceOptionsStmt(
            conv_altertablespaceoptionsstmt(mcx, n.cast())?,
        )),
        tags::T_AlterTableMoveAllStmt => {
            Ok(Node::AlterTableMoveAllStmt(conv_altertablemoveallstmt(mcx, n.cast())?))
        }
        tags::T_AlterExtensionStmt => {
            Ok(Node::AlterExtensionStmt(conv_alterextensionstmt(mcx, n.cast())?))
        }
        tags::T_AlterExtensionContentsStmt => Ok(Node::AlterExtensionContentsStmt(
            conv_alterextensioncontentsstmt(mcx, n.cast())?,
        )),
        tags::T_AlterFdwStmt => Ok(Node::AlterFdwStmt(conv_alterfdwstmt(mcx, n.cast())?)),
        tags::T_AlterForeignServerStmt => {
            Ok(Node::AlterForeignServerStmt(conv_alterforeignserverstmt(mcx, n.cast())?))
        }
        tags::T_AlterUserMappingStmt => {
            Ok(Node::AlterUserMappingStmt(conv_alterusermappingstmt(mcx, n.cast())?))
        }
        tags::T_AlterPolicyStmt => {
            Ok(Node::AlterPolicyStmt(conv_alterpolicystmt(mcx, n.cast())?))
        }
        tags::T_AlterDatabaseStmt => {
            Ok(Node::AlterDatabaseStmt(conv_alterdatabasestmt(mcx, n.cast())?))
        }
        tags::T_AlterDatabaseRefreshCollStmt => Ok(Node::AlterDatabaseRefreshCollStmt(
            conv_alterdatabaserefreshcollstmt(mcx, n.cast())?,
        )),
        tags::T_AlterDatabaseSetStmt => {
            Ok(Node::AlterDatabaseSetStmt(conv_alterdatabasesetstmt(mcx, n.cast())?))
        }
        tags::T_AlterTSDictionaryStmt => {
            Ok(Node::AlterTSDictionaryStmt(conv_altertsdictionarystmt(mcx, n.cast())?))
        }
        tags::T_AlterTSConfigurationStmt => Ok(Node::AlterTSConfigurationStmt(
            conv_altertsconfigurationstmt(mcx, n.cast())?,
        )),
        tags::T_AlterPublicationStmt => {
            Ok(Node::AlterPublicationStmt(conv_alterpublicationstmt(mcx, n.cast())?))
        }
        tags::T_AlterSubscriptionStmt => {
            Ok(Node::AlterSubscriptionStmt(conv_altersubscriptionstmt(mcx, n.cast())?))
        }

        // --- utility / GRANT / transaction family (F4) ---
        tags::T_GrantStmt => Ok(Node::GrantStmt(conv_grantstmt(mcx, n.cast())?)),
        tags::T_GrantRoleStmt => Ok(Node::GrantRoleStmt(conv_grantrolestmt(mcx, n.cast())?)),
        tags::T_VariableSetStmt => Ok(Node::VariableSetStmt(conv_variablesetstmt(mcx, n.cast())?)),
        tags::T_VariableShowStmt => {
            Ok(Node::VariableShowStmt(conv_variableshowstmt(mcx, n.cast())?))
        }
        tags::T_TransactionStmt => Ok(Node::TransactionStmt(conv_transactionstmt(mcx, n.cast())?)),
        tags::T_CopyStmt => Ok(Node::CopyStmt(conv_copystmt(mcx, n.cast())?)),
        tags::T_ExplainStmt => Ok(Node::ExplainStmt(conv_explainstmt(mcx, n.cast())?)),
        tags::T_PrepareStmt => Ok(Node::PrepareStmt(conv_preparestmt(mcx, n.cast())?)),
        tags::T_ExecuteStmt => Ok(Node::ExecuteStmt(conv_executestmt(mcx, n.cast())?)),
        tags::T_DeallocateStmt => Ok(Node::DeallocateStmt(conv_deallocatestmt(mcx, n.cast())?)),
        tags::T_DeclareCursorStmt => {
            Ok(Node::DeclareCursorStmt(conv_declarecursorstmt(mcx, n.cast())?))
        }
        tags::T_ClosePortalStmt => Ok(Node::ClosePortalStmt(conv_closeportalstmt(mcx, n.cast())?)),
        tags::T_FetchStmt => Ok(Node::FetchStmt(conv_fetchstmt(mcx, n.cast())?)),
        tags::T_VacuumStmt => Ok(Node::VacuumStmt(conv_vacuumstmt(mcx, n.cast())?)),
        tags::T_VacuumRelation => Ok(Node::VacuumRelation(conv_vacuumrelation(mcx, n.cast())?)),
        tags::T_ClusterStmt => Ok(Node::ClusterStmt(conv_clusterstmt(mcx, n.cast())?)),
        tags::T_ReindexStmt => Ok(Node::ReindexStmt(conv_reindexstmt(mcx, n.cast())?)),
        tags::T_CheckPointStmt => Ok(Node::CheckPointStmt(tdn::CheckPointStmt)),
        tags::T_DiscardStmt => Ok(Node::DiscardStmt(conv_discardstmt(n.cast()))),
        tags::T_LockStmt => Ok(Node::LockStmt(conv_lockstmt(mcx, n.cast())?)),
        tags::T_ConstraintsSetStmt => {
            Ok(Node::ConstraintsSetStmt(conv_constraintssetstmt(mcx, n.cast())?))
        }
        tags::T_LoadStmt => Ok(Node::LoadStmt(conv_loadstmt(mcx, n.cast())?)),
        tags::T_TruncateStmt => Ok(Node::TruncateStmt(conv_truncatestmt(mcx, n.cast())?)),
        tags::T_CommentStmt => Ok(Node::CommentStmt(conv_commentstmt(mcx, n.cast())?)),
        tags::T_SecLabelStmt => Ok(Node::SecLabelStmt(conv_seclabelstmt(mcx, n.cast())?)),
        tags::T_RuleStmt => Ok(Node::RuleStmt(conv_rulestmt(mcx, n.cast())?)),
        tags::T_NotifyStmt => Ok(Node::NotifyStmt(conv_notifystmt(mcx, n.cast())?)),
        tags::T_ListenStmt => Ok(Node::ListenStmt(conv_listenstmt(mcx, n.cast())?)),
        tags::T_UnlistenStmt => Ok(Node::UnlistenStmt(conv_unlistenstmt(mcx, n.cast())?)),
        tags::T_DoStmt => Ok(Node::DoStmt(conv_dostmt(mcx, n.cast())?)),
        tags::T_CallStmt => Ok(Node::CallStmt(conv_callstmt(mcx, n.cast())?)),
        tags::T_RefreshMatViewStmt => {
            Ok(Node::RefreshMatViewStmt(conv_refreshmatviewstmt(mcx, n.cast())?))
        }
        tags::T_AlterSystemStmt => Ok(Node::AlterSystemStmt(conv_altersystemstmt(mcx, n.cast())?)),
        tags::T_DropdbStmt => Ok(Node::DropdbStmt(conv_dropdbstmt(mcx, n.cast())?)),
        tags::T_DropRoleStmt => Ok(Node::DropRoleStmt(conv_droprolestmt(mcx, n.cast())?)),
        tags::T_DropTableSpaceStmt => {
            Ok(Node::DropTableSpaceStmt(conv_droptablespacestmt(mcx, n.cast())?))
        }
        tags::T_CreateFdwStmt => Ok(Node::CreateFdwStmt(conv_createfdwstmt(mcx, n.cast())?)),
        tags::T_CreateForeignServerStmt => Ok(Node::CreateForeignServerStmt(
            conv_createforeignserverstmt(mcx, n.cast())?,
        )),
        tags::T_CreateForeignTableStmt => Ok(Node::CreateForeignTableStmt(
            conv_createforeigntablestmt(mcx, n.cast())?,
        )),
        tags::T_CreateUserMappingStmt => {
            Ok(Node::CreateUserMappingStmt(conv_createusermappingstmt(mcx, n.cast())?))
        }
        tags::T_DropUserMappingStmt => {
            Ok(Node::DropUserMappingStmt(conv_dropusermappingstmt(mcx, n.cast())?))
        }
        tags::T_ImportForeignSchemaStmt => Ok(Node::ImportForeignSchemaStmt(
            conv_importforeignschemastmt(mcx, n.cast())?,
        )),
        tags::T_CreatePolicyStmt => Ok(Node::CreatePolicyStmt(conv_createpolicystmt(mcx, n.cast())?)),
        tags::T_PublicationTable => Ok(Node::PublicationTable(conv_publicationtable(mcx, n.cast())?)),
        tags::T_PublicationObjSpec => {
            Ok(Node::PublicationObjSpec(conv_publicationobjspec(mcx, n.cast())?))
        }
        tags::T_CreatePublicationStmt => {
            Ok(Node::CreatePublicationStmt(conv_createpublicationstmt(mcx, n.cast())?))
        }
        tags::T_CreateSubscriptionStmt => Ok(Node::CreateSubscriptionStmt(
            conv_createsubscriptionstmt(mcx, n.cast())?,
        )),
        tags::T_DropSubscriptionStmt => {
            Ok(Node::DropSubscriptionStmt(conv_dropsubscriptionstmt(mcx, n.cast())?))
        }
        tags::T_CreateEventTrigStmt => {
            Ok(Node::CreateEventTrigStmt(conv_createeventtrigstmt(mcx, n.cast())?))
        }
        tags::T_AlterEventTrigStmt => {
            Ok(Node::AlterEventTrigStmt(conv_altereventtrigstmt(mcx, n.cast())?))
        }
        tags::T_CreateTransformStmt => {
            Ok(Node::CreateTransformStmt(conv_createtransformstmt(mcx, n.cast())?))
        }
        tags::T_ReturnStmt => Ok(Node::ReturnStmt(conv_returnstmt(mcx, n.cast())?)),
        tags::T_PLAssignStmt => Ok(Node::PLAssignStmt(conv_plassignstmt(mcx, n.cast())?)),

        // --- anything else: the absent DDL/utility node families ---
        other => unported(other, node_tag_name(other)),
    }
}

include!("convert_stmts.rs");
include!("convert_exprs.rs");
include!("convert_misc.rs");
include!("convert_ddl.rs");
