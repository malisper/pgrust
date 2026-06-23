//! `_read<Type>` readers for the read_parse_family node arms. Each reader reads its fields in
//! the exact order the OUT side wrote them. `try_read` returns `Some(result)`
//! iff this family owns `label`.
//!
//! Mirror of `out_parse_family` — see that module's header for the covered
//! node inventory. `COMMONTABLEEXPR` reads its `search_clause` (a typed
//! `CTESearchClause`, not a `Node` arm) as a framed `{CTESEARCHCLAUSE ...}`
//! sub-node directly, mirroring the OUT side.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_error::PgResult;
use nodes::copy_query::{Query, QuerySource};
use nodes::jointype::JoinType;
use nodes::modifytable::{MergeMatchKind, OverridingKind};
use nodes::nodelimit::LimitOption;
use nodes::nodes::{CmdType, Node, OnConflictAction};
use nodes::nodesamplescan::TableSampleClause;
use nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
use nodes::primnodes::Expr;
use nodes::rawnodes::{
    A_ArrayExpr, A_Const, A_Expr, A_Expr_Kind, A_Indices, A_Indirection, A_Star, Alias,
    CTECycleClause, CTEMaterialize, CTESearchClause, CommonTableExpr, CollateClause, ColumnDef,
    ColumnRef, DeleteStmt, FromExpr,
    FuncCall, GroupingSet, GroupingSetKind, InferClause, InsertStmt, JoinExpr, LockClauseStrength,
    LockWaitPolicy, LockingClause, MergeAction, MergeStmt, MergeWhenClause, MultiAssignRef,
    OnConflictClause, OnConflictExpr, ParamRef, RangeFunction, RangeSubselect, RangeTableSample,
    RangeTblFunction, RangeTblRef, RangeVar, ResTarget, ReturningClause, RowMarkClause,
    SelectStmt, SetOperation, SetOperationStmt, SortBy, SortByDir, SortByNulls, SortGroupClause,
    TypeCast, TypeName, UpdateStmt, WCOKind, WindowClause, WindowDef, WithCheckOption, WithClause,
};

use crate::{
    atoi_i64, elog_error, read_bool_field, read_char_field, read_enum_field,
    read_float_field, read_int_field, read_location_field, read_node_field, read_node_list_field,
    read_oid_field, read_uint64_field, read_uint_field, tok_str,
};
use nodes_core::read::{self, Token};

type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;

// ---------------------------------------------------------------------------
// Local primitives (the lib's `next_token`/`read_string_field`/expr-list
// helpers are private; replicate them here over the public `read::` cursor).
// ---------------------------------------------------------------------------

/// Pull the next token off the shared cursor, erroring on premature EOF.
fn next_token<'a>() -> PgResult<Token<'a>> {
    read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))
}

/// `READ_STRING_FIELD` (`nullable_string`): `<>` (length 0) → C NULL (`None`);
/// `""` → empty string; otherwise `debackslash`.
fn read_string_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgString<'mcx>>> {
    let _label = next_token()?;
    let v = next_token()?;
    if v.bytes.is_empty() {
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(PgString::from_str_in("", mcx)?));
    }
    let s = read::debackslash(v.bytes);
    Ok(Some(PgString::from_str_in(&s, mcx)?))
}

/// `READ_NODE_FIELD` of a `List *` of nodes into an owned `PgVec<NodePtr>`.
fn read_node_vec_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let items = read_node_list_field(mcx)?;
    let mut v = mcx::vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        v.push(it);
    }
    Ok(v)
}

/// `READ_NODE_FIELD` of an `Oid` scalar list `(o ...)` (or `<>` for NIL). The
/// core `node_read` errors on a top-level `(o ...)`, so consume the tokens
/// directly: skip the `:label`, then read either `<>` or `( o num... )`.
fn read_oid_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u32>> {
    read_scalar_list_u32(mcx, b'o')
}

/// `READ_NODE_FIELD` of an `int` scalar list `(i ...)` (or `<>` for NIL).
fn read_int_scalar_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, i32>> {
    let raw = read_scalar_list_u32(mcx, b'i')?;
    let mut v = mcx::vec_with_capacity_in(mcx, raw.len())?;
    for x in raw.iter() {
        v.push(*x as i32);
    }
    Ok(v)
}

/// Shared scalar-list reader. `disc` is the expected discriminator byte
/// (`o`/`i`/`x`). Reads the `:label`, then `<>` (empty) or `( <disc> n... )`.
/// The numbers are parsed as i64 then truncated to u32 (signed ints round-trip
/// through their two's-complement u32 image).
fn read_scalar_list_u32<'mcx>(mcx: Mcx<'mcx>, disc: u8) -> PgResult<PgVec<'mcx, u32>> {
    let _label = next_token()?; // :fldname
    let first = next_token()?;
    if first.bytes.is_empty() {
        // `<>` — C NIL.
        return Ok(PgVec::new_in(mcx));
    }
    if first.bytes != b"(" {
        return Err(elog_error("unrecognized token: expected '(' for scalar list"));
    }
    let d = next_token()?;
    if d.bytes.len() != 1 || d.bytes[0] != disc {
        return Err(elog_error("unrecognized token: wrong scalar-list discriminator"));
    }
    let mut v = PgVec::new_in(mcx);
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        let n = atoi_i64(&tok_str(&t));
        v.push(n as u32);
    }
    Ok(v)
}

/// `READ_NODE_FIELD` of an optional framed child whose struct is NOT a `Node`
/// arm: read the node, match the expected arm, unwrap into an `mcx` box.
/// `<>` → `None`.
fn read_opt_box<'mcx, T>(
    mcx: Mcx<'mcx>,
    extract: impl FnOnce(Node<'mcx>) -> Option<T>,
) -> PgResult<Option<PgBox<'mcx, T>>> {
    match read_node_field(mcx)? {
        None => Ok(None),
        Some(n) => match extract(PgBox::into_inner(n)) {
            Some(v) => Ok(Some(mcx::alloc_in(mcx, v)?)),
            None => Err(elog_error("unexpected node type for framed child field")),
        },
    }
}

/// `READ_NODE_FIELD` of an `Option<NodePtr>` (`Node *`): the child or `None`.
fn read_opt_node<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<NodePtr<'mcx>>> {
    read_node_field(mcx)
}

/// `READ_NODE_FIELD` of a `List *` of `Expr` (`(expr ...)` or `<>`). The core
/// `node_read` rebuilds it as a `Node::List` of `Node::Expr`.
fn read_expr_list<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Expr>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(Vec::new()),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elements) => {
                let mut out = Vec::with_capacity(elements.len());
                for cell in elements {
                    {
            let __n = PgBox::into_inner(cell);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(e),
                None => {
                            return Err(elog_error(alloc::format!(
                                "expected Expr element in arg list, got {:?}",
                                __tag
                            )))
                        },
            }
        }
                }
                Ok(out)
            },
                None => Err(elog_error(alloc::format!(
                "expected List for expr-list field, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` of a single `Expr *` (`{...}` or `<>`).
fn read_opt_expr_boxed<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<Box<Expr>>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => Ok(Some(Box::new(e))),
                None => Err(elog_error(alloc::format!(
                "expected Expr child, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` of a single expression-only `Node *` field that is
/// concretely typed `Option<PgBox<Expr>>` on the `Query` (havingQual /
/// limitOffset / limitCount / mergeJoinCondition). The serialized form is the
/// inner `Expr` node (the C wrote `(Node *) expr`), so unwrap `Node::Expr`.
fn read_opt_expr_box<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgBox<'mcx, Expr>>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(None),
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => Ok(Some(mcx::alloc_in(mcx, e)?)),
                None => Err(elog_error(alloc::format!(
                "expected Expr child, got {:?}",
                __tag
            ))),
            }
        },
    }
}

/// `READ_NODE_FIELD` of a `List *` of `Expr` carried as `PgVec<PgBox<Expr>>`,
/// matching `write_box_expr_list_field`: `(...)` of framed Exprs, `<>` → `None`.
fn read_box_expr_list_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, PgBox<'mcx, Expr>>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None); // `<>` — NIL
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for expr list"));
    }
    let mut out = PgVec::new_in(mcx);
    let mut cur = next_token()?;
    loop {
        if cur.bytes == b")" {
            break;
        }
        let child = read::node_read(mcx, Some(cur))?;
        match child {
            Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(mcx::alloc_in(mcx, e)?),
                None => {
                    return Err(elog_error(alloc::format!(
                        "expected Expr in list, got {:?}",
                        __tag
                    )))
                },
            }
        },
            None => return Err(elog_error("unexpected null in non-nullable expr list")),
        }
        cur = next_token()?;
    }
    Ok(Some(out))
}

/// `READ_NODE_FIELD` of a `List *` of `Expr` with NULL cells, carried as
/// `PgVec<Option<PgBox<Expr>>>`, matching `write_opt_box_expr_list_field`:
/// `(...)` of framed Exprs or `<>` cells; a top-level `<>` → `None`.
fn read_opt_box_expr_list_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Option<PgBox<'mcx, Expr>>>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None); // `<>` — NIL
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for nullable expr list"));
    }
    let mut out = PgVec::new_in(mcx);
    let mut cur = next_token()?;
    loop {
        if cur.bytes == b")" {
            break;
        }
        // A `<>` cell is the C NULL element.
        let child = read::node_read(mcx, Some(cur))?;
        match child {
            None => out.push(None),
            Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(Some(mcx::alloc_in(mcx, e)?)),
                None => {
                    return Err(elog_error(alloc::format!(
                        "expected Expr in nullable list, got {:?}",
                        __tag
                    )))
                },
            }
        },
        }
        cur = next_token()?;
    }
    Ok(Some(out))
}

/// `READ_NODE_FIELD` of a `List *` of `String` value nodes carried as
/// `PgVec<PgString>`, matching `write_pgstring_list_field`: `("a" "b" ...)`;
/// `<>` → `None`.
fn read_pgstring_list_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None);
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for string list"));
    }
    let mut out = PgVec::new_in(mcx);
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        out.push(read_string_token(mcx, &t)?);
    }
    Ok(Some(out))
}

/// `READ_NODE_FIELD` of a `List *` of `String` value nodes with NULL cells,
/// carried as `PgVec<Option<PgString>>`, matching `write_opt_pgstring_list_field`:
/// `("a" <> ...)`; `<>` (top level) → `None`.
fn read_opt_pgstring_list_opt<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Option<PgString<'mcx>>>>> {
    let _label = next_token()?;
    let open = next_token()?;
    if open.bytes.is_empty() {
        return Ok(None);
    }
    if open.bytes != b"(" {
        return Err(elog_error("expected '(' for nullable string list"));
    }
    let mut out = PgVec::new_in(mcx);
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        if t.bytes.is_empty() {
            out.push(None); // `<>` cell — DEFAULT namespace
        } else {
            out.push(Some(read_string_token(mcx, &t)?));
        }
    }
    Ok(Some(out))
}

/// `READ_NODE_FIELD` of an `Oid` scalar list as `Option` (NIL `<>` → `None`,
/// distinguishing it from a present-but-empty `(o)` list, for byte-stability).
fn read_oid_list_opt<'mcx>(mcx: Mcx<'mcx>, disc: u8) -> PgResult<Option<PgVec<'mcx, u32>>> {
    let _label = next_token()?;
    let first = next_token()?;
    if first.bytes.is_empty() {
        return Ok(None); // `<>` — NIL
    }
    if first.bytes != b"(" {
        return Err(elog_error("expected '(' for scalar list"));
    }
    let d = next_token()?;
    if d.bytes.len() != 1 || d.bytes[0] != disc {
        return Err(elog_error("wrong scalar-list discriminator"));
    }
    let mut v = PgVec::new_in(mcx);
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        v.push(atoi_i64(&tok_str(&t)) as u32);
    }
    Ok(Some(v))
}

/// As [`read_oid_list_opt`] but yielding `Option<PgVec<i32>>` for an IntList.
fn read_int_list_opt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, i32>>> {
    match read_oid_list_opt(mcx, b'i')? {
        None => Ok(None),
        Some(raw) => {
            let mut v = mcx::vec_with_capacity_in(mcx, raw.len())?;
            for x in raw.iter() {
                v.push(*x as i32);
            }
            Ok(Some(v))
        }
    }
}

/// Decode a `_outString` value-node token (`"..."`, kept whole by `pg_strtok`):
/// debackslash, then strip the surrounding quotes.
fn read_string_token<'mcx>(mcx: Mcx<'mcx>, t: &Token<'_>) -> PgResult<PgString<'mcx>> {
    let s = read::debackslash(t.bytes);
    let trimmed = s
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .unwrap_or(&s);
    Ok(PgString::from_str_in(trimmed, mcx)?)
}

// ---------------------------------------------------------------------------
// Direct-value list readers (PgVec<RangeTblEntry> / <RTEPermissionInfo> /
// <TargetEntry>): read a node list, match each framed element's arm.
// ---------------------------------------------------------------------------

fn read_rte_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, RangeTblEntry<'mcx>>> {
    let items = read_node_list_field(mcx)?;
    let mut v = mcx::vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        {
            let __n = PgBox::into_inner(it);
            let __tag = __n.node_tag();
            match __n.into_rangetblentry() {
                Some(r) => v.push(r),
                None => {
                return Err(elog_error(alloc::format!(
                    "expected RangeTblEntry in rtable, got {:?}",
                    __tag
                )))
            },
            }
        }
    }
    Ok(v)
}

fn read_rteperminfo_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, RTEPermissionInfo<'mcx>>> {
    let items = read_node_list_field(mcx)?;
    let mut v = mcx::vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        {
            let __n = PgBox::into_inner(it);
            let __tag = __n.node_tag();
            match __n.into_rtepermissioninfo() {
                Some(p) => v.push(p),
                None => {
                return Err(elog_error(alloc::format!(
                    "expected RTEPermissionInfo in rteperminfos, got {:?}",
                    __tag
                )))
            },
            }
        }
    }
    Ok(v)
}

fn read_te_vec<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, nodes::primnodes::TargetEntry<'mcx>>> {
    let items = read_node_list_field(mcx)?;
    let mut v = mcx::vec_with_capacity_in(mcx, items.len())?;
    for it in items {
        {
            let __n = PgBox::into_inner(it);
            let __tag = __n.node_tag();
            match __n.into_targetentry() {
                Some(t) => v.push(t),
                None => {
                return Err(elog_error(alloc::format!(
                    "expected TargetEntry in targetList, got {:?}",
                    __tag
                )))
            },
            }
        }
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
// Enum decoders.
// ---------------------------------------------------------------------------

fn cmd_type_from(c: i32) -> CmdType {
    match c {
        0 => CmdType::CMD_UNKNOWN,
        1 => CmdType::CMD_SELECT,
        2 => CmdType::CMD_UPDATE,
        3 => CmdType::CMD_INSERT,
        4 => CmdType::CMD_DELETE,
        5 => CmdType::CMD_MERGE,
        6 => CmdType::CMD_UTILITY,
        _ => CmdType::CMD_NOTHING,
    }
}

fn query_source_from(c: i32) -> QuerySource {
    match c {
        0 => QuerySource::QSRC_ORIGINAL,
        1 => QuerySource::QSRC_PARSER,
        2 => QuerySource::QSRC_INSTEAD_RULE,
        3 => QuerySource::QSRC_QUAL_INSTEAD_RULE,
        _ => QuerySource::QSRC_NON_INSTEAD_RULE,
    }
}

fn overriding_from(c: i32) -> OverridingKind {
    match c {
        0 => OverridingKind::OVERRIDING_NOT_SET,
        1 => OverridingKind::OVERRIDING_USER_VALUE,
        _ => OverridingKind::OVERRIDING_SYSTEM_VALUE,
    }
}

fn limit_option_from(c: i32) -> LimitOption {
    match c {
        1 => LimitOption::LIMIT_OPTION_WITH_TIES,
        _ => LimitOption::LIMIT_OPTION_COUNT,
    }
}

fn join_type_from(c: i32) -> JoinType {
    match c {
        0 => JoinType::JOIN_INNER,
        1 => JoinType::JOIN_LEFT,
        2 => JoinType::JOIN_FULL,
        3 => JoinType::JOIN_RIGHT,
        4 => JoinType::JOIN_SEMI,
        5 => JoinType::JOIN_ANTI,
        6 => JoinType::JOIN_RIGHT_SEMI,
        7 => JoinType::JOIN_RIGHT_ANTI,
        8 => JoinType::JOIN_UNIQUE_OUTER,
        _ => JoinType::JOIN_UNIQUE_INNER,
    }
}

fn set_operation_from(c: i32) -> SetOperation {
    match c {
        0 => SetOperation::SETOP_NONE,
        1 => SetOperation::SETOP_UNION,
        2 => SetOperation::SETOP_INTERSECT,
        _ => SetOperation::SETOP_EXCEPT,
    }
}

fn grouping_set_kind_from(c: i32) -> GroupingSetKind {
    match c {
        0 => GroupingSetKind::GROUPING_SET_EMPTY,
        1 => GroupingSetKind::GROUPING_SET_SIMPLE,
        2 => GroupingSetKind::GROUPING_SET_ROLLUP,
        3 => GroupingSetKind::GROUPING_SET_CUBE,
        _ => GroupingSetKind::GROUPING_SET_SETS,
    }
}

fn wco_kind_from(c: i32) -> WCOKind {
    match c {
        0 => WCOKind::WCO_VIEW_CHECK,
        1 => WCOKind::WCO_RLS_INSERT_CHECK,
        2 => WCOKind::WCO_RLS_UPDATE_CHECK,
        3 => WCOKind::WCO_RLS_CONFLICT_CHECK,
        4 => WCOKind::WCO_RLS_MERGE_UPDATE_CHECK,
        _ => WCOKind::WCO_RLS_MERGE_DELETE_CHECK,
    }
}

fn lock_strength_from(c: i32) -> LockClauseStrength {
    match c {
        0 => LockClauseStrength::LCS_NONE,
        1 => LockClauseStrength::LCS_FORKEYSHARE,
        2 => LockClauseStrength::LCS_FORSHARE,
        3 => LockClauseStrength::LCS_FORNOKEYUPDATE,
        _ => LockClauseStrength::LCS_FORUPDATE,
    }
}

fn lock_wait_from(c: i32) -> LockWaitPolicy {
    match c {
        0 => LockWaitPolicy::LockWaitBlock,
        1 => LockWaitPolicy::LockWaitSkip,
        _ => LockWaitPolicy::LockWaitError,
    }
}

fn sortby_dir_from(c: i32) -> SortByDir {
    match c {
        0 => SortByDir::SORTBY_DEFAULT,
        1 => SortByDir::SORTBY_ASC,
        2 => SortByDir::SORTBY_DESC,
        _ => SortByDir::SORTBY_USING,
    }
}

fn sortby_nulls_from(c: i32) -> SortByNulls {
    match c {
        0 => SortByNulls::SORTBY_NULLS_DEFAULT,
        1 => SortByNulls::SORTBY_NULLS_FIRST,
        _ => SortByNulls::SORTBY_NULLS_LAST,
    }
}

fn on_conflict_action_from(c: i32) -> OnConflictAction {
    match c {
        0 => OnConflictAction::ONCONFLICT_NONE,
        1 => OnConflictAction::ONCONFLICT_NOTHING,
        _ => OnConflictAction::ONCONFLICT_UPDATE,
    }
}

fn merge_match_kind_from(c: i32) -> MergeMatchKind {
    match c {
        0 => MergeMatchKind::MERGE_WHEN_MATCHED,
        1 => MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
        _ => MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET,
    }
}

fn coercion_form_from(c: i32) -> nodes::primnodes::CoercionForm {
    use nodes::primnodes::CoercionForm;
    match c {
        0 => CoercionForm::COERCE_EXPLICIT_CALL,
        1 => CoercionForm::COERCE_EXPLICIT_CAST,
        2 => CoercionForm::COERCE_IMPLICIT_CAST,
        _ => CoercionForm::COERCE_SQL_SYNTAX,
    }
}

// ===========================================================================
// _readQuery
// ===========================================================================

fn read_query<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Query<'mcx>> {
    let mut q = Query::new(mcx);
    q.commandType = cmd_type_from(read_enum_field()?);
    q.querySource = query_source_from(read_enum_field()?);
    q.canSetTag = read_bool_field()?;
    q.utilityStmt = read_opt_node(mcx)?;
    q.resultRelation = read_int_field()?;
    q.hasAggs = read_bool_field()?;
    q.hasWindowFuncs = read_bool_field()?;
    q.hasTargetSRFs = read_bool_field()?;
    q.hasSubLinks = read_bool_field()?;
    q.hasDistinctOn = read_bool_field()?;
    q.hasRecursive = read_bool_field()?;
    q.hasModifyingCTE = read_bool_field()?;
    q.hasForUpdate = read_bool_field()?;
    q.hasRowSecurity = read_bool_field()?;
    q.hasGroupRTE = read_bool_field()?;
    q.isReturn = read_bool_field()?;
    q.cteList = read_node_vec_field(mcx)?;
    q.rtable = read_rte_vec(mcx)?;
    q.rteperminfos = read_rteperminfo_vec(mcx)?;
    q.jointree = read_opt_box(mcx, |n| n.into_fromexpr())?;
    q.mergeActionList = read_node_vec_field(mcx)?;
    q.mergeTargetRelation = read_int_field()?;
    q.mergeJoinCondition = read_opt_expr_box(mcx)?;
    q.targetList = read_te_vec(mcx)?;
    q.r#override = overriding_from(read_enum_field()?);
    q.onConflict = read_opt_box(mcx, |n| n.into_onconflictexpr())?;
    q.returningOldAlias = read_string_field(mcx)?;
    q.returningNewAlias = read_string_field(mcx)?;
    q.returningList = read_te_vec(mcx)?;
    q.has_returning_list = !q.returningList.is_empty();
    q.groupClause = read_node_vec_field(mcx)?;
    q.groupDistinct = read_bool_field()?;
    q.groupingSets = read_node_vec_field(mcx)?;
    q.havingQual = read_opt_expr_box(mcx)?;
    q.windowClause = read_node_vec_field(mcx)?;
    q.distinctClause = read_node_vec_field(mcx)?;
    q.sortClause = read_node_vec_field(mcx)?;
    q.limitOffset = read_opt_expr_box(mcx)?;
    q.limitCount = read_opt_expr_box(mcx)?;
    q.limitOption = limit_option_from(read_enum_field()?);
    q.rowMarks = read_node_vec_field(mcx)?;
    q.setOperations = read_opt_node(mcx)?;
    q.constraintDeps = read_oid_list_field(mcx)?;
    q.withCheckOptions = read_node_vec_field(mcx)?;
    q.stmt_location = read_location_field()?;
    q.stmt_len = read_location_field()?;
    Ok(q)
}

// ===========================================================================
// _readRangeTblEntry — custom, switch on rtekind.
// ===========================================================================

fn read_range_tbl_entry<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeTblEntry<'mcx>> {
    let mut r = RangeTblEntry::new_in(mcx);
    r.alias = read_opt_box(mcx, |n| n.into_alias())?;
    r.eref = read_opt_box(mcx, |n| n.into_alias())?;
    r.rtekind = rtekind_from(read_enum_field()?);

    match r.rtekind {
        RTEKind::RTE_RELATION => {
            r.relid = read_oid_field()?;
            r.inh = read_bool_field()?;
            r.relkind = read_char_field()? as i8;
            r.rellockmode = read_int_field()?;
            r.perminfoindex = read_uint_field()?;
            r.tablesample = read_opt_node(mcx)?;
        }
        RTEKind::RTE_SUBQUERY => {
            r.subquery = read_opt_box(mcx, |n| n.into_query())?;
            r.security_barrier = read_bool_field()?;
            r.relid = read_oid_field()?;
            r.inh = read_bool_field()?;
            r.relkind = read_char_field()? as i8;
            r.rellockmode = read_int_field()?;
            r.perminfoindex = read_uint_field()?;
        }
        RTEKind::RTE_JOIN => {
            r.jointype = join_type_from(read_enum_field()?);
            r.joinmergedcols = read_int_field()?;
            r.joinaliasvars = read_node_vec_field(mcx)?;
            r.joinleftcols = read_int_scalar_list_field(mcx)?;
            r.joinrightcols = read_int_scalar_list_field(mcx)?;
            r.join_using_alias = read_opt_box(mcx, |n| n.into_alias())?;
        }
        RTEKind::RTE_FUNCTION => {
            r.functions = read_node_vec_field(mcx)?;
            r.funcordinality = read_bool_field()?;
        }
        RTEKind::RTE_TABLEFUNC => {
            r.tablefunc = read_opt_node(mcx)?;
            // C copies coltypes/coltypmods/colcollations from the tablefunc node
            // (a post-read derivation, not a serialized RTE field); the out side
            // does not write them here, so leave the RTE copies empty (NIL).
        }
        RTEKind::RTE_VALUES => {
            r.values_lists = read_node_vec_field(mcx)?;
            r.coltypes = read_oid_list_field(mcx)?;
            r.coltypmods = read_int_scalar_list_field(mcx)?;
            r.colcollations = read_oid_list_field(mcx)?;
        }
        RTEKind::RTE_CTE => {
            r.ctename = read_string_field(mcx)?;
            r.ctelevelsup = read_uint_field()?;
            r.self_reference = read_bool_field()?;
            r.coltypes = read_oid_list_field(mcx)?;
            r.coltypmods = read_int_scalar_list_field(mcx)?;
            r.colcollations = read_oid_list_field(mcx)?;
        }
        RTEKind::RTE_NAMEDTUPLESTORE => {
            r.enrname = read_string_field(mcx)?;
            r.enrtuples = read_float_field()?;
            r.coltypes = read_oid_list_field(mcx)?;
            r.coltypmods = read_int_scalar_list_field(mcx)?;
            r.colcollations = read_oid_list_field(mcx)?;
            r.relid = read_oid_field()?;
        }
        RTEKind::RTE_RESULT => {
            // nothing
        }
        RTEKind::RTE_GROUP => {
            r.groupexprs = read_node_vec_field(mcx)?;
        }
    }

    r.lateral = read_bool_field()?;
    r.inFromCl = read_bool_field()?;
    r.securityQuals = read_node_vec_field(mcx)?;
    Ok(r)
}

fn rtekind_from(c: i32) -> RTEKind {
    match c {
        0 => RTEKind::RTE_RELATION,
        1 => RTEKind::RTE_SUBQUERY,
        2 => RTEKind::RTE_JOIN,
        3 => RTEKind::RTE_FUNCTION,
        4 => RTEKind::RTE_TABLEFUNC,
        5 => RTEKind::RTE_VALUES,
        6 => RTEKind::RTE_CTE,
        7 => RTEKind::RTE_NAMEDTUPLESTORE,
        8 => RTEKind::RTE_RESULT,
        _ => RTEKind::RTE_GROUP,
    }
}

// ===========================================================================
// _readRTEPermissionInfo
// ===========================================================================

fn read_rte_perm_info<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RTEPermissionInfo<'mcx>> {
    let relid = read_oid_field()?;
    let inh = read_bool_field()?;
    let requiredPerms = read_uint64_field()?;
    let checkAsUser = read_oid_field()?;
    let selectedCols = crate::read_bitmapset_opt_field(mcx)?;
    let insertedCols = crate::read_bitmapset_opt_field(mcx)?;
    let updatedCols = crate::read_bitmapset_opt_field(mcx)?;
    Ok(RTEPermissionInfo {
        relid,
        inh,
        requiredPerms,
        checkAsUser,
        selectedCols,
        insertedCols,
        updatedCols,
    })
}

// ===========================================================================
// _readRangeTblFunction
// ===========================================================================

fn read_range_tbl_function<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeTblFunction<'mcx>> {
    let funcexpr = read_opt_node(mcx)?;
    let funccolcount = read_int_field()?;
    let funccolnames = read_node_vec_field(mcx)?;
    let funccoltypes = read_oid_list_field(mcx)?;
    let funccoltypmods = read_int_scalar_list_field(mcx)?;
    let funccolcollations = read_oid_list_field(mcx)?;
    let funcparams = crate::read_bitmapset_opt_field(mcx)?;
    Ok(RangeTblFunction {
        funcexpr,
        funccolcount,
        funccolnames,
        funccoltypes,
        funccoltypmods,
        funccolcollations,
        funcparams,
    })
}

// ===========================================================================
// _readTableSampleClause
// ===========================================================================

fn read_table_sample_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<TableSampleClause<'mcx>> {
    let tsmhandler = read_oid_field()?;
    let args_vec = read_expr_list(mcx)?;
    let args = {
        let mut out: PgVec<'mcx, Expr> = mcx::vec_with_capacity_in(mcx, args_vec.len())?;
        for a in args_vec {
            out.push(a);
        }
        Some(out)
    };
    let repeatable = read_opt_expr_boxed(mcx)?;
    Ok(TableSampleClause {
        tsmhandler,
        args,
        repeatable,
        ..Default::default()
    })
}

// ===========================================================================
// _readSortGroupClause
// ===========================================================================

fn read_sort_group_clause() -> PgResult<SortGroupClause> {
    let tleSortGroupRef = read_uint_field()?;
    let eqop = read_oid_field()?;
    let sortop = read_oid_field()?;
    let reverse_sort = read_bool_field()?;
    let nulls_first = read_bool_field()?;
    let hashable = read_bool_field()?;
    Ok(SortGroupClause {
        tleSortGroupRef,
        eqop,
        sortop,
        reverse_sort,
        nulls_first,
        hashable,
    })
}

// ===========================================================================
// _readGroupingSet
// ===========================================================================

fn read_grouping_set<'mcx>(mcx: Mcx<'mcx>) -> PgResult<GroupingSet<'mcx>> {
    let kind = grouping_set_kind_from(read_enum_field()?);
    let content = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(GroupingSet {
        kind,
        content,
        location,
    })
}

// ===========================================================================
// _readWindowClause
// ===========================================================================

fn read_window_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<WindowClause<'mcx>> {
    let name = read_string_field(mcx)?;
    let refname = read_string_field(mcx)?;
    let partitionClause = read_node_vec_field(mcx)?;
    let orderClause = read_node_vec_field(mcx)?;
    let frameOptions = read_int_field()?;
    let startOffset = read_opt_node(mcx)?;
    let endOffset = read_opt_node(mcx)?;
    let startInRangeFunc = read_oid_field()?;
    let endInRangeFunc = read_oid_field()?;
    let inRangeColl = read_oid_field()?;
    let inRangeAsc = read_bool_field()?;
    let inRangeNullsFirst = read_bool_field()?;
    let winref = read_uint_field()?;
    let copiedOrder = read_bool_field()?;
    Ok(WindowClause {
        name,
        refname,
        partitionClause,
        orderClause,
        frameOptions,
        startOffset,
        endOffset,
        startInRangeFunc,
        endInRangeFunc,
        inRangeColl,
        inRangeAsc,
        inRangeNullsFirst,
        winref,
        copiedOrder,
    })
}

// ===========================================================================
// _readRowMarkClause
// ===========================================================================

fn read_row_mark_clause() -> PgResult<RowMarkClause> {
    let rti = read_uint_field()?;
    let strength = lock_strength_from(read_enum_field()?);
    let waitPolicy = lock_wait_from(read_enum_field()?);
    let pushedDown = read_bool_field()?;
    Ok(RowMarkClause {
        rti,
        strength,
        waitPolicy,
        pushedDown,
    })
}

// ===========================================================================
// _readWithCheckOption
// ===========================================================================

fn read_with_check_option<'mcx>(mcx: Mcx<'mcx>) -> PgResult<WithCheckOption<'mcx>> {
    let kind = wco_kind_from(read_enum_field()?);
    let relname = read_string_field(mcx)?;
    let polname = read_string_field(mcx)?;
    let qual = read_opt_node(mcx)?;
    let cascaded = read_bool_field()?;
    Ok(WithCheckOption {
        kind,
        relname,
        polname,
        qual,
        cascaded,
    })
}

// ===========================================================================
// _readCTECycleClause
// ===========================================================================

fn read_cte_cycle_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CTECycleClause<'mcx>> {
    let cycle_col_list = read_node_vec_field(mcx)?;
    let cycle_mark_column = read_string_field(mcx)?;
    let cycle_mark_value = read_opt_node(mcx)?;
    let cycle_mark_default = read_opt_node(mcx)?;
    let cycle_path_column = read_string_field(mcx)?;
    let location = read_location_field()?;
    let cycle_mark_type = read_oid_field()?;
    let cycle_mark_typmod = read_int_field()?;
    let cycle_mark_collation = read_oid_field()?;
    let cycle_mark_neop = read_oid_field()?;
    Ok(CTECycleClause {
        cycle_col_list,
        cycle_mark_column,
        cycle_mark_value,
        cycle_mark_default,
        cycle_path_column,
        location,
        cycle_mark_type,
        cycle_mark_typmod,
        cycle_mark_collation,
        cycle_mark_neop,
    })
}

fn cte_materialize_from(c: i32) -> CTEMaterialize {
    match c {
        0 => CTEMaterialize::CTEMaterializeDefault,
        1 => CTEMaterialize::CTEMaterializeAlways,
        _ => CTEMaterialize::CTEMaterializeNever,
    }
}

/// `_readCTESearchClause` (readfuncs.funcs.c) — read the body of a framed
/// `{CTESEARCHCLAUSE ...}` node (the `{` and `CTESEARCHCLAUSE` label already
/// consumed by the caller). `CTESearchClause` is a typed struct, not a `Node`
/// arm, so it is read directly here (mirroring the OUT side's framed field).
fn read_cte_search_clause_body<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CTESearchClause<'mcx>> {
    let search_col_list = read_node_vec_field(mcx)?;
    let search_breadth_first = read_bool_field()?;
    let search_seq_column = read_string_field(mcx)?;
    let location = read_location_field()?;
    Ok(CTESearchClause {
        search_col_list,
        search_breadth_first,
        search_seq_column,
        location,
    })
}

/// `READ_NODE_FIELD(search_clause)` over the framed `{CTESEARCHCLAUSE ...}` /
/// `<>` form. Reads the `:search_clause` label then either `<>` (None) or the
/// framed body directly (CTESearchClause is not a `Node` arm, so it cannot go
/// through `node_read`).
fn read_search_clause_field<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, CTESearchClause<'mcx>>>> {
    let _label = next_token()?; // skip :search_clause
    let t = next_token()?;
    if t.bytes.is_empty() {
        // `<>` — NULL.
        return Ok(None);
    }
    if t.bytes != b"{" {
        return Err(elog_error(
            "readCommonTableExpr: expected '{' or '<>' for search_clause".to_string(),
        ));
    }
    let label = next_token()?;
    if label.bytes != b"CTESEARCHCLAUSE" {
        return Err(elog_error(
            "readCommonTableExpr: search_clause is not a CTESEARCHCLAUSE node".to_string(),
        ));
    }
    let body = read_cte_search_clause_body(mcx)?;
    let close = next_token()?;
    if close.bytes != b"}" {
        return Err(elog_error(
            "readCommonTableExpr: expected '}' after CTESEARCHCLAUSE body".to_string(),
        ));
    }
    Ok(Some(mcx::alloc_in(mcx, body)?))
}

/// `_readCommonTableExpr` (readfuncs.funcs.c).
fn read_common_table_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CommonTableExpr<'mcx>> {
    let ctename = read_string_field(mcx)?;
    let aliascolnames = read_node_vec_field(mcx)?;
    let ctematerialized = cte_materialize_from(read_enum_field()?);
    let ctequery = read_opt_node(mcx)?;
    let search_clause = read_search_clause_field(mcx)?;
    let cycle_clause = read_opt_node(mcx)?;
    let location = read_location_field()?;
    let cterecursive = read_bool_field()?;
    let cterefcount = read_int_field()?;
    let ctecolnames = read_node_vec_field(mcx)?;
    let ctecoltypes = read_oid_list_field(mcx)?;
    let ctecoltypmods = read_int_scalar_list_field(mcx)?;
    let ctecolcollations = read_oid_list_field(mcx)?;
    Ok(CommonTableExpr {
        ctename,
        aliascolnames,
        ctematerialized,
        ctequery,
        search_clause,
        cycle_clause,
        location,
        cterecursive,
        cterefcount,
        ctecolnames,
        ctecoltypes,
        ctecoltypmods,
        ctecolcollations,
    })
}

// ===========================================================================
// _readSetOperationStmt
// ===========================================================================

fn read_set_operation_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SetOperationStmt<'mcx>> {
    let op = set_operation_from(read_enum_field()?);
    let all = read_bool_field()?;
    let larg = read_opt_node(mcx)?;
    let rarg = read_opt_node(mcx)?;
    let colTypes = read_oid_list_field(mcx)?;
    let colTypmods = read_int_scalar_list_field(mcx)?;
    let colCollations = read_oid_list_field(mcx)?;
    let groupClauses = read_node_vec_field(mcx)?;
    Ok(SetOperationStmt {
        op,
        all,
        larg,
        rarg,
        colTypes,
        colTypmods,
        colCollations,
        groupClauses,
    })
}

// ===========================================================================
// _readAlias / _readRangeVar / _readTypeName / _readColumnDef
// ===========================================================================

fn read_alias<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Alias<'mcx>> {
    let aliasname = read_string_field(mcx)?;
    let colnames = read_node_vec_field(mcx)?;
    Ok(Alias {
        aliasname,
        colnames,
    })
}

fn read_range_var<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeVar<'mcx>> {
    let catalogname = read_string_field(mcx)?;
    let schemaname = read_string_field(mcx)?;
    let relname = read_string_field(mcx)?;
    let inh = read_bool_field()?;
    let relpersistence = read_char_field()? as i8;
    let alias = read_opt_box(mcx, |n| n.into_alias())?;
    let location = read_location_field()?;
    Ok(RangeVar {
        catalogname,
        schemaname,
        relname,
        inh,
        relpersistence,
        alias,
        location,
    })
}

fn read_type_name<'mcx>(mcx: Mcx<'mcx>) -> PgResult<TypeName<'mcx>> {
    let names = read_node_vec_field(mcx)?;
    let typeOid = read_oid_field()?;
    let setof = read_bool_field()?;
    let pct_type = read_bool_field()?;
    let typmods = read_node_vec_field(mcx)?;
    let typemod = read_int_field()?;
    let arrayBounds = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(TypeName {
        names,
        typeOid,
        setof,
        pct_type,
        typmods,
        typemod,
        arrayBounds,
        location,
    })
}

fn read_column_def<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ColumnDef<'mcx>> {
    let colname = read_string_field(mcx)?;
    let typeName = read_opt_box(mcx, |n| n.into_typename())?;
    let compression = read_string_field(mcx)?;
    let inhcount = read_int_field()? as i16;
    let is_local = read_bool_field()?;
    let is_not_null = read_bool_field()?;
    let is_from_type = read_bool_field()?;
    let storage = read_char_field()? as i8;
    let storage_name = read_string_field(mcx)?;
    let raw_default = read_opt_node(mcx)?;
    let cooked_default = read_opt_node(mcx)?;
    let identity = read_char_field()? as i8;
    let identitySequence = read_opt_box(mcx, |n| n.into_rangevar())?;
    let generated = read_char_field()? as i8;
    let collClause = read_opt_box(mcx, |n| n.into_collateclause())?;
    let collOid = read_oid_field()?;
    let constraints = read_node_vec_field(mcx)?;
    let fdwoptions = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(ColumnDef {
        colname,
        typeName,
        compression,
        inhcount,
        is_local,
        is_not_null,
        is_from_type,
        storage,
        storage_name,
        raw_default,
        cooked_default,
        identity,
        identitySequence,
        generated,
        collClause,
        collOid,
        constraints,
        fdwoptions,
        location,
    })
}

// ===========================================================================
// _readRangeTblRef / _readJoinExpr / _readFromExpr / _readOnConflictExpr
// ===========================================================================

fn read_range_tbl_ref() -> PgResult<RangeTblRef> {
    let rtindex = read_int_field()?;
    Ok(RangeTblRef { rtindex })
}

fn read_join_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<JoinExpr<'mcx>> {
    let jointype = join_type_from(read_enum_field()?);
    let isNatural = read_bool_field()?;
    let larg = read_opt_node(mcx)?;
    let rarg = read_opt_node(mcx)?;
    let usingClause = read_node_vec_field(mcx)?;
    let join_using_alias = read_opt_box(mcx, |n| n.into_alias())?;
    let quals = read_opt_node(mcx)?;
    let alias = read_opt_box(mcx, |n| n.into_alias())?;
    let rtindex = read_int_field()?;
    Ok(JoinExpr {
        jointype,
        isNatural,
        larg,
        rarg,
        usingClause,
        join_using_alias,
        quals,
        alias,
        rtindex,
    })
}

fn read_from_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FromExpr<'mcx>> {
    let fromlist = read_node_vec_field(mcx)?;
    let quals = read_opt_node(mcx)?;
    Ok(FromExpr { fromlist, quals })
}

fn read_on_conflict_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<OnConflictExpr<'mcx>> {
    let action = on_conflict_action_from(read_enum_field()?);
    let arbiterElems = read_node_vec_field(mcx)?;
    let arbiterWhere = read_opt_node(mcx)?;
    let constraint = read_oid_field()?;
    let onConflictSet = read_node_vec_field(mcx)?;
    let onConflictWhere = read_opt_node(mcx)?;
    let exclRelIndex = read_int_field()?;
    let exclRelTlist = read_node_vec_field(mcx)?;
    Ok(OnConflictExpr {
        action,
        arbiterElems,
        arbiterWhere,
        constraint,
        onConflictSet,
        onConflictWhere,
        exclRelIndex,
        exclRelTlist,
    })
}

// ===========================================================================
// _readMergeAction / _readLockingClause
// ===========================================================================

fn read_merge_action<'mcx>(mcx: Mcx<'mcx>) -> PgResult<MergeAction<'mcx>> {
    let matchKind = merge_match_kind_from(read_enum_field()?);
    let commandType = cmd_type_from(read_enum_field()?);
    let r#override = overriding_from(read_enum_field()?);
    let qual = read_opt_node(mcx)?;
    let targetList = read_node_vec_field(mcx)?;
    let updateColnos = read_int_scalar_list_field(mcx)?;
    Ok(MergeAction {
        matchKind,
        commandType,
        r#override,
        qual,
        targetList,
        updateColnos,
    })
}

fn read_locking_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<LockingClause<'mcx>> {
    let lockedRels = read_node_vec_field(mcx)?;
    let strength = lock_strength_from(read_enum_field()?);
    let waitPolicy = lock_wait_from(read_enum_field()?);
    Ok(LockingClause {
        lockedRels,
        strength,
        waitPolicy,
    })
}

// ===========================================================================
// _readColumnRef / _readParamRef / _readA_Expr / _readFuncCall
// ===========================================================================

fn read_column_ref<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ColumnRef<'mcx>> {
    let fields = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(ColumnRef { fields, location })
}

fn read_param_ref() -> PgResult<ParamRef> {
    let number = read_int_field()?;
    let location = read_location_field()?;
    Ok(ParamRef { number, location })
}

fn read_a_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<A_Expr<'mcx>> {
    // Peek the first token: an operator keyword chooses the kind (and the next
    // thing is `:name <list>`); a `:name` label means AEXPR_OP (and we read the
    // value directly).
    let tok = next_token()?;
    let (kind, name): (A_Expr_Kind, PgVec<'mcx, NodePtr<'mcx>>) = match tok.bytes {
        b"ANY" => (A_Expr_Kind::AEXPR_OP_ANY, read_node_vec_field(mcx)?),
        b"ALL" => (A_Expr_Kind::AEXPR_OP_ALL, read_node_vec_field(mcx)?),
        b"DISTINCT" => (A_Expr_Kind::AEXPR_DISTINCT, read_node_vec_field(mcx)?),
        b"NOT_DISTINCT" => (A_Expr_Kind::AEXPR_NOT_DISTINCT, read_node_vec_field(mcx)?),
        b"NULLIF" => (A_Expr_Kind::AEXPR_NULLIF, read_node_vec_field(mcx)?),
        b"IN" => (A_Expr_Kind::AEXPR_IN, read_node_vec_field(mcx)?),
        b"LIKE" => (A_Expr_Kind::AEXPR_LIKE, read_node_vec_field(mcx)?),
        b"ILIKE" => (A_Expr_Kind::AEXPR_ILIKE, read_node_vec_field(mcx)?),
        b"SIMILAR" => (A_Expr_Kind::AEXPR_SIMILAR, read_node_vec_field(mcx)?),
        b"BETWEEN" => (A_Expr_Kind::AEXPR_BETWEEN, read_node_vec_field(mcx)?),
        b"NOT_BETWEEN" => (A_Expr_Kind::AEXPR_NOT_BETWEEN, read_node_vec_field(mcx)?),
        b"BETWEEN_SYM" => (A_Expr_Kind::AEXPR_BETWEEN_SYM, read_node_vec_field(mcx)?),
        b"NOT_BETWEEN_SYM" => (A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM, read_node_vec_field(mcx)?),
        b":name" => {
            // AEXPR_OP: the peeked token WAS the :name label, so read the value
            // (a `(...)` list of String nodes) directly via node_read.
            let name = match read::node_read(mcx, None)? {
                None => PgVec::new_in(mcx),
                Some(n) => {
                    let node = PgBox::into_inner(n);
                    if node.is_list() {
                        let elements = node.into_list().unwrap();
                        let mut v = mcx::vec_with_capacity_in(mcx, elements.len())?;
                        for c in elements {
                            v.push(c);
                        }
                        v
                    } else {
                        let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
                        v.push(mcx::alloc_in(mcx, node)?);
                        v
                    }
                }
            };
            (A_Expr_Kind::AEXPR_OP, name)
        }
        other => {
            return Err(elog_error(alloc::format!(
                "unrecognized A_Expr discriminator token: {:?}",
                String::from_utf8_lossy(other)
            )))
        }
    };
    let lexpr = read_opt_node(mcx)?;
    let rexpr = read_opt_node(mcx)?;
    let rexpr_list_start = read_location_field()?;
    let rexpr_list_end = read_location_field()?;
    let location = read_location_field()?;
    Ok(A_Expr {
        kind,
        name,
        lexpr,
        rexpr,
        rexpr_list_start,
        rexpr_list_end,
        location,
    })
}

fn read_func_call<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FuncCall<'mcx>> {
    let funcname = read_node_vec_field(mcx)?;
    let args = read_node_vec_field(mcx)?;
    let agg_order = read_node_vec_field(mcx)?;
    let agg_filter = read_opt_node(mcx)?;
    let over = read_opt_box(mcx, |n| n.into_windowdef())?;
    let agg_within_group = read_bool_field()?;
    let agg_star = read_bool_field()?;
    let agg_distinct = read_bool_field()?;
    let func_variadic = read_bool_field()?;
    let funcformat = coercion_form_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(FuncCall {
        funcname,
        args,
        agg_order,
        agg_filter,
        over,
        agg_within_group,
        agg_star,
        agg_distinct,
        func_variadic,
        funcformat,
        location,
    })
}

// ===========================================================================
// _readA_Star / _readA_Indices / _readA_Indirection / _readA_ArrayExpr
// ===========================================================================

fn read_a_star() -> PgResult<A_Star> {
    Ok(A_Star)
}

fn read_a_indices<'mcx>(mcx: Mcx<'mcx>) -> PgResult<A_Indices<'mcx>> {
    let is_slice = read_bool_field()?;
    let lidx = read_opt_node(mcx)?;
    let uidx = read_opt_node(mcx)?;
    Ok(A_Indices {
        is_slice,
        lidx,
        uidx,
    })
}

fn read_a_indirection<'mcx>(mcx: Mcx<'mcx>) -> PgResult<A_Indirection<'mcx>> {
    let arg = read_opt_node(mcx)?;
    let indirection = read_node_vec_field(mcx)?;
    Ok(A_Indirection { arg, indirection })
}

fn read_a_array_expr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<A_ArrayExpr<'mcx>> {
    let elements = read_node_vec_field(mcx)?;
    let list_start = read_location_field()?;
    let list_end = read_location_field()?;
    let location = read_location_field()?;
    Ok(A_ArrayExpr {
        elements,
        list_start,
        list_end,
        location,
    })
}

// ===========================================================================
// _readResTarget / _readMultiAssignRef / _readTypeCast / _readCollateClause
// ===========================================================================

fn read_res_target<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ResTarget<'mcx>> {
    let name = read_string_field(mcx)?;
    let indirection = read_node_vec_field(mcx)?;
    let val = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(ResTarget {
        name,
        indirection,
        val,
        location,
    })
}

fn read_multi_assign_ref<'mcx>(mcx: Mcx<'mcx>) -> PgResult<MultiAssignRef<'mcx>> {
    let source = read_opt_node(mcx)?;
    let colno = read_int_field()?;
    let ncolumns = read_int_field()?;
    Ok(MultiAssignRef {
        source,
        colno,
        ncolumns,
    })
}

fn read_type_cast<'mcx>(mcx: Mcx<'mcx>) -> PgResult<TypeCast<'mcx>> {
    let arg = read_opt_node(mcx)?;
    let typeName = read_opt_box(mcx, |n| n.into_typename())?;
    let location = read_location_field()?;
    Ok(TypeCast {
        arg,
        typeName,
        location,
    })
}

fn read_collate_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<CollateClause<'mcx>> {
    let arg = read_opt_node(mcx)?;
    let collname = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(CollateClause {
        arg,
        collname,
        location,
    })
}

// ===========================================================================
// _readSortBy / _readWindowDef
// ===========================================================================

fn read_sort_by<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SortBy<'mcx>> {
    let node = read_opt_node(mcx)?;
    let sortby_dir = sortby_dir_from(read_enum_field()?);
    let sortby_nulls = sortby_nulls_from(read_enum_field()?);
    let useOp = read_node_vec_field(mcx)?;
    let location = read_location_field()?;
    Ok(SortBy {
        node,
        sortby_dir,
        sortby_nulls,
        useOp,
        location,
    })
}

fn read_window_def<'mcx>(mcx: Mcx<'mcx>) -> PgResult<WindowDef<'mcx>> {
    let name = read_string_field(mcx)?;
    let refname = read_string_field(mcx)?;
    let partitionClause = read_node_vec_field(mcx)?;
    let orderClause = read_node_vec_field(mcx)?;
    let frameOptions = read_int_field()?;
    let startOffset = read_opt_node(mcx)?;
    let endOffset = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(WindowDef {
        name,
        refname,
        partitionClause,
        orderClause,
        frameOptions,
        startOffset,
        endOffset,
        location,
    })
}

// ===========================================================================
// _readRangeSubselect / _readRangeFunction / _readRangeTableSample
// ===========================================================================

fn read_range_subselect<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeSubselect<'mcx>> {
    let lateral = read_bool_field()?;
    let subquery = read_opt_node(mcx)?;
    let alias = read_opt_box(mcx, |n| n.into_alias())?;
    Ok(RangeSubselect {
        lateral,
        subquery,
        alias,
    })
}

fn read_range_function<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeFunction<'mcx>> {
    let lateral = read_bool_field()?;
    let ordinality = read_bool_field()?;
    let is_rowsfrom = read_bool_field()?;
    let functions = read_node_vec_field(mcx)?;
    let alias = read_opt_box(mcx, |n| n.into_alias())?;
    let coldeflist = read_node_vec_field(mcx)?;
    Ok(RangeFunction {
        lateral,
        ordinality,
        is_rowsfrom,
        functions,
        alias,
        coldeflist,
    })
}

fn read_range_table_sample<'mcx>(mcx: Mcx<'mcx>) -> PgResult<RangeTableSample<'mcx>> {
    let relation = read_opt_node(mcx)?;
    let method = read_node_vec_field(mcx)?;
    let args = read_node_vec_field(mcx)?;
    let repeatable = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(RangeTableSample {
        relation,
        method,
        args,
        repeatable,
        location,
    })
}

// ===========================================================================
// _readWithClause / _readInferClause / _readOnConflictClause
// ===========================================================================

fn read_with_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<WithClause<'mcx>> {
    let ctes = read_node_vec_field(mcx)?;
    let recursive = read_bool_field()?;
    let location = read_location_field()?;
    Ok(WithClause {
        ctes,
        recursive,
        location,
    })
}

fn read_infer_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<InferClause<'mcx>> {
    let indexElems = read_node_vec_field(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let conname = read_string_field(mcx)?;
    let location = read_location_field()?;
    Ok(InferClause {
        indexElems,
        whereClause,
        conname,
        location,
    })
}

fn read_on_conflict_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<OnConflictClause<'mcx>> {
    let action = on_conflict_action_from(read_enum_field()?);
    let infer = read_opt_box(mcx, |n| n.into_inferclause())?;
    let targetList = read_node_vec_field(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(OnConflictClause {
        action,
        infer,
        targetList,
        whereClause,
        location,
    })
}

// ===========================================================================
// _readMergeWhenClause / _readReturningClause
// ===========================================================================

fn read_merge_when_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<MergeWhenClause<'mcx>> {
    let matchKind = merge_match_kind_from(read_enum_field()?);
    let commandType = cmd_type_from(read_enum_field()?);
    let r#override = overriding_from(read_enum_field()?);
    let condition = read_opt_node(mcx)?;
    let targetList = read_node_vec_field(mcx)?;
    let values = read_node_vec_field(mcx)?;
    Ok(MergeWhenClause {
        matchKind,
        commandType,
        r#override,
        condition,
        targetList,
        values,
    })
}

fn read_returning_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<ReturningClause<'mcx>> {
    let options = read_node_vec_field(mcx)?;
    let exprs = read_node_vec_field(mcx)?;
    Ok(ReturningClause { options, exprs })
}

// ===========================================================================
// _readInsertStmt / _readDeleteStmt / _readUpdateStmt / _readMergeStmt
// ===========================================================================

fn read_insert_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<InsertStmt<'mcx>> {
    let relation = read_opt_box(mcx, |n| n.into_rangevar())?;
    let cols = read_node_vec_field(mcx)?;
    let selectStmt = read_opt_node(mcx)?;
    let onConflictClause = read_opt_box(mcx, |n| n.into_onconflictclause())?;
    let returningClause = read_opt_box(mcx, |n| n.into_returningclause())?;
    let withClause = read_opt_box(mcx, |n| n.into_withclause())?;
    let r#override = overriding_from(read_enum_field()?);
    Ok(InsertStmt {
        relation,
        cols,
        selectStmt,
        onConflictClause,
        returningClause,
        withClause,
        r#override,
    })
}

fn read_delete_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<DeleteStmt<'mcx>> {
    let relation = read_opt_box(mcx, |n| n.into_rangevar())?;
    let usingClause = read_node_vec_field(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let returningClause = read_opt_box(mcx, |n| n.into_returningclause())?;
    let withClause = read_opt_box(mcx, |n| n.into_withclause())?;
    Ok(DeleteStmt {
        relation,
        usingClause,
        whereClause,
        returningClause,
        withClause,
    })
}

fn read_update_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<UpdateStmt<'mcx>> {
    let relation = read_opt_box(mcx, |n| n.into_rangevar())?;
    let targetList = read_node_vec_field(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let fromClause = read_node_vec_field(mcx)?;
    let returningClause = read_opt_box(mcx, |n| n.into_returningclause())?;
    let withClause = read_opt_box(mcx, |n| n.into_withclause())?;
    Ok(UpdateStmt {
        relation,
        targetList,
        whereClause,
        fromClause,
        returningClause,
        withClause,
    })
}

fn read_merge_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<MergeStmt<'mcx>> {
    let relation = read_opt_box(mcx, |n| n.into_rangevar())?;
    let sourceRelation = read_opt_node(mcx)?;
    let joinCondition = read_opt_node(mcx)?;
    let mergeWhenClauses = read_node_vec_field(mcx)?;
    let returningClause = read_opt_box(mcx, |n| n.into_returningclause())?;
    let withClause = read_opt_box(mcx, |n| n.into_withclause())?;
    Ok(MergeStmt {
        relation,
        sourceRelation,
        joinCondition,
        mergeWhenClauses,
        returningClause,
        withClause,
    })
}

// ===========================================================================
// _readSelectStmt
// ===========================================================================

fn read_select_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SelectStmt<'mcx>> {
    let distinctClause = read_node_vec_field(mcx)?;
    let intoClause = read_opt_node(mcx)?;
    let targetList = read_node_vec_field(mcx)?;
    let fromClause = read_node_vec_field(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let groupClause = read_node_vec_field(mcx)?;
    let groupDistinct = read_bool_field()?;
    let havingClause = read_opt_node(mcx)?;
    let windowClause = read_node_vec_field(mcx)?;
    let valuesLists = read_node_vec_field(mcx)?;
    let sortClause = read_node_vec_field(mcx)?;
    let limitOffset = read_opt_node(mcx)?;
    let limitCount = read_opt_node(mcx)?;
    let limitOption = limit_option_from(read_enum_field()?);
    let lockingClause = read_node_vec_field(mcx)?;
    let withClause = read_opt_box(mcx, |n| n.into_withclause())?;
    let op = set_operation_from(read_enum_field()?);
    let all = read_bool_field()?;
    let larg = read_opt_box(mcx, |n| n.into_selectstmt())?;
    let rarg = read_opt_box(mcx, |n| n.into_selectstmt())?;
    Ok(SelectStmt {
        distinctClause,
        intoClause,
        targetList,
        fromClause,
        whereClause,
        groupClause,
        groupDistinct,
        havingClause,
        windowClause,
        valuesLists,
        sortClause,
        limitOffset,
        limitCount,
        limitOption,
        lockingClause,
        withClause,
        op,
        all,
        larg,
        rarg,
    })
}

// ===========================================================================
// _readA_Const — custom.
// ===========================================================================

fn read_a_const<'mcx>(mcx: Mcx<'mcx>) -> PgResult<A_Const<'mcx>> {
    // Peek the first token: "NULL" → isnull; else it is the ":val" label and
    // the value node follows.
    let tok = next_token()?;
    let (val, isnull) = if tok.bytes == b"NULL" {
        (None, true)
    } else {
        // tok was the ":val" label; read the value node directly.
        let v = read::node_read(mcx, None)?;
        (v, false)
    };
    let location = read_location_field()?;
    Ok(A_Const {
        val,
        isnull,
        location,
    })
}

/// `_readTableFunc` — reads the fields `out_table_func` wrote, in order.
pub(crate) fn read_table_func<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<nodes::primnodes::TableFunc<'mcx>> {
    use nodes::primnodes::TableFuncType;
    let functype = match read_enum_field()? {
        0 => TableFuncType::TFT_XMLTABLE,
        _ => TableFuncType::TFT_JSON_TABLE,
    };
    let ns_uris = read_box_expr_list_opt(mcx)?;
    let ns_names = read_opt_pgstring_list_opt(mcx)?;
    let docexpr = read_opt_expr_box(mcx)?;
    let rowexpr = read_opt_expr_box(mcx)?;
    let colnames = read_pgstring_list_opt(mcx)?;
    let coltypes = read_oid_list_opt(mcx, b'o')?;
    let coltypmods = read_int_list_opt(mcx)?;
    let colcollations = read_oid_list_opt(mcx, b'o')?;
    let colexprs = read_opt_box_expr_list_opt(mcx)?;
    let coldefexprs = read_opt_box_expr_list_opt(mcx)?;
    let colvalexprs = read_opt_box_expr_list_opt(mcx)?;
    let passingvalexprs = read_box_expr_list_opt(mcx)?;
    let notnulls = crate::read_bitmapset_opt_field(mcx)?;
    let plan = read_opt_node(mcx)?;
    let ordinalitycol = read_int_field()?;
    let location = read_location_field()?;
    Ok(nodes::primnodes::TableFunc {
        functype,
        ns_uris,
        ns_names,
        docexpr,
        rowexpr,
        colnames,
        coltypes,
        coltypmods,
        colcollations,
        colexprs,
        coldefexprs,
        colvalexprs,
        passingvalexprs,
        notnulls,
        plan,
        ordinalitycol,
        location,
    })
}

/// `_readJsonTablePathScan` — the `JsonTablePlan` leaf reader. Mirrors
/// `out_json_table_path_scan` exactly: `path` (node), `name` (string),
/// `errorOnError` (bool), `child` (opt node), `colMin`/`colMax` (ints).
///
/// Note: the C `_readJsonTablePathScan` reads `path` as a `JsonTablePath`
/// node, but this port's outfuncs collapses the `JsonTablePath` wrapper into
/// its `value` (Const) + `name` fields, so we read those two directly in the
/// same order they were written.
pub(crate) fn read_json_table_path_scan<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<nodes::primnodes::JsonTablePathScan<'mcx>> {
    let path = read_node_field(mcx)?
        .ok_or_else(|| elog_error("JsonTablePathScan.path must not be NULL"))?;
    let name = read_string_field(mcx)?;
    let errorOnError = crate::read_bool_field()?;
    let child = read_opt_node(mcx)?;
    let colMin = crate::read_int_field()?;
    let colMax = crate::read_int_field()?;
    Ok(nodes::primnodes::JsonTablePathScan {
        path,
        name,
        errorOnError,
        child,
        colMin,
        colMax,
    })
}

/// `_readJsonTableSiblingJoin` — the sibling-join plan reader. Mirrors
/// `out_json_table_sibling_join`: `lplan` (node), `rplan` (node).
pub(crate) fn read_json_table_sibling_join<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<nodes::primnodes::JsonTableSiblingJoin<'mcx>> {
    let lplan = read_node_field(mcx)?
        .ok_or_else(|| elog_error("JsonTableSiblingJoin.lplan must not be NULL"))?;
    let rplan = read_node_field(mcx)?
        .ok_or_else(|| elog_error("JsonTableSiblingJoin.rplan must not be NULL"))?;
    Ok(nodes::primnodes::JsonTableSiblingJoin { lplan, rplan })
}

// ===========================================================================
// Dispatch.
// ===========================================================================

/// Dispatch the read_parse_family LABELs this module owns.
pub(crate) fn try_read<'mcx>(mcx: Mcx<'mcx>, label: &[u8]) -> Option<PgResult<Node<'mcx>>> {
    let r: PgResult<Node<'mcx>> = match label {
        b"QUERY" => read_query(mcx).and_then(|p| Node::mk_query(mcx, p)),
        b"RANGETBLENTRY" => read_range_tbl_entry(mcx).and_then(|p| Node::mk_range_tbl_entry(mcx, p)),
        b"RTEPERMISSIONINFO" => read_rte_perm_info(mcx).and_then(|p| Node::mk_rte_permission_info(mcx, p)),
        b"RANGETBLFUNCTION" => read_range_tbl_function(mcx).and_then(|p| Node::mk_range_tbl_function(mcx, p)),
        b"TABLESAMPLECLAUSE" => read_table_sample_clause(mcx).and_then(|p| Node::mk_table_sample_clause(mcx, p)),
        b"SORTGROUPCLAUSE" => read_sort_group_clause().and_then(|p| Node::mk_sort_group_clause(mcx, p)),
        b"GROUPINGSET" => read_grouping_set(mcx).and_then(|p| Node::mk_grouping_set(mcx, p)),
        b"WINDOWCLAUSE" => read_window_clause(mcx).and_then(|p| Node::mk_window_clause(mcx, p)),
        b"ROWMARKCLAUSE" => read_row_mark_clause().and_then(|p| Node::mk_row_mark_clause(mcx, p)),
        b"WITHCHECKOPTION" => read_with_check_option(mcx).and_then(|p| Node::mk_with_check_option(mcx, p)),
        b"CTECYCLECLAUSE" => read_cte_cycle_clause(mcx).and_then(|p| Node::mk_cte_cycle_clause(mcx, p)),
        b"SETOPERATIONSTMT" => read_set_operation_stmt(mcx).and_then(|p| Node::mk_set_operation_stmt(mcx, p)),
        b"ALIAS" => read_alias(mcx).and_then(|p| Node::mk_alias(mcx, p)),
        b"RANGEVAR" => read_range_var(mcx).and_then(|p| Node::mk_range_var(mcx, p)),
        b"TYPENAME" => read_type_name(mcx).and_then(|p| Node::mk_type_name(mcx, p)),
        b"COLUMNDEF" => read_column_def(mcx).and_then(|p| Node::mk_column_def(mcx, p)),
        b"RANGETBLREF" => read_range_tbl_ref().and_then(|p| Node::mk_range_tbl_ref(mcx, p)),
        b"JOINEXPR" => read_join_expr(mcx).and_then(|p| Node::mk_join_expr(mcx, p)),
        b"FROMEXPR" => read_from_expr(mcx).and_then(|p| Node::mk_from_expr(mcx, p)),
        b"ONCONFLICTEXPR" => read_on_conflict_expr(mcx).and_then(|p| Node::mk_on_conflict_expr(mcx, p)),
        b"MERGEACTION" => read_merge_action(mcx).and_then(|p| Node::mk_merge_action(mcx, p)),
        b"LOCKINGCLAUSE" => read_locking_clause(mcx).and_then(|p| Node::mk_locking_clause(mcx, p)),
        b"COLUMNREF" => read_column_ref(mcx).and_then(|p| Node::mk_column_ref(mcx, p)),
        b"PARAMREF" => read_param_ref().and_then(|p| Node::mk_param_ref(mcx, p)),
        b"A_EXPR" => read_a_expr(mcx).and_then(|p| Node::mk_a_expr(mcx, p)),
        b"FUNCCALL" => read_func_call(mcx).and_then(|p| Node::mk_func_call(mcx, p)),
        b"A_STAR" => read_a_star().and_then(|p| Node::mk_a_star(mcx, p)),
        b"A_INDICES" => read_a_indices(mcx).and_then(|p| Node::mk_a_indices(mcx, p)),
        b"A_INDIRECTION" => read_a_indirection(mcx).and_then(|p| Node::mk_a_indirection(mcx, p)),
        b"A_ARRAYEXPR" => read_a_array_expr(mcx).and_then(|p| Node::mk_a_array_expr(mcx, p)),
        b"RESTARGET" => read_res_target(mcx).and_then(|p| Node::mk_res_target(mcx, p)),
        b"MULTIASSIGNREF" => read_multi_assign_ref(mcx).and_then(|p| Node::mk_multi_assign_ref(mcx, p)),
        b"TYPECAST" => read_type_cast(mcx).and_then(|p| Node::mk_type_cast(mcx, p)),
        b"COLLATECLAUSE" => read_collate_clause(mcx).and_then(|p| Node::mk_collate_clause(mcx, p)),
        b"SORTBY" => read_sort_by(mcx).and_then(|p| Node::mk_sort_by(mcx, p)),
        b"WINDOWDEF" => read_window_def(mcx).and_then(|p| Node::mk_window_def(mcx, p)),
        b"RANGESUBSELECT" => read_range_subselect(mcx).and_then(|p| Node::mk_range_subselect(mcx, p)),
        b"RANGEFUNCTION" => read_range_function(mcx).and_then(|p| Node::mk_range_function(mcx, p)),
        b"RANGETABLESAMPLE" => read_range_table_sample(mcx).and_then(|p| Node::mk_range_table_sample(mcx, p)),
        b"WITHCLAUSE" => read_with_clause(mcx).and_then(|p| Node::mk_with_clause(mcx, p)),
        b"INFERCLAUSE" => read_infer_clause(mcx).and_then(|p| Node::mk_infer_clause(mcx, p)),
        b"ONCONFLICTCLAUSE" => read_on_conflict_clause(mcx).and_then(|p| Node::mk_on_conflict_clause(mcx, p)),
        b"MERGEWHENCLAUSE" => read_merge_when_clause(mcx).and_then(|p| Node::mk_merge_when_clause(mcx, p)),
        b"RETURNINGCLAUSE" => read_returning_clause(mcx).and_then(|p| Node::mk_returning_clause(mcx, p)),
        b"INSERTSTMT" => read_insert_stmt(mcx).and_then(|p| Node::mk_insert_stmt(mcx, p)),
        b"DELETESTMT" => read_delete_stmt(mcx).and_then(|p| Node::mk_delete_stmt(mcx, p)),
        b"UPDATESTMT" => read_update_stmt(mcx).and_then(|p| Node::mk_update_stmt(mcx, p)),
        b"MERGESTMT" => read_merge_stmt(mcx).and_then(|p| Node::mk_merge_stmt(mcx, p)),
        b"SELECTSTMT" => read_select_stmt(mcx).and_then(|p| Node::mk_select_stmt(mcx, p)),
        b"A_CONST" => read_a_const(mcx).and_then(|p| Node::mk_a_const(mcx, p)),

        b"TABLEFUNC" => read_table_func(mcx).and_then(|p| Node::mk_table_func(mcx, p)),

        b"JSONTABLEPATHSCAN" => {
            read_json_table_path_scan(mcx).and_then(|p| Node::mk_json_table_path_scan(mcx, p))
        }
        b"JSONTABLESIBLINGJOIN" => {
            read_json_table_sibling_join(mcx).and_then(|p| Node::mk_json_table_sibling_join(mcx, p))
        }

        b"COMMONTABLEEXPR" => read_common_table_expr(mcx).and_then(|p| Node::mk_common_table_expr(mcx, p)),

        _ => return None,
    };
    Some(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ensure_seams_for_tests as ensure_seams;
    use nodes_core::read::string_to_node;
    use outfuncs::nodeToString;
    use mcx::MemoryContext;
    use nodes::primnodes::VarReturningType;

    fn assert_framed_round_trip(node: &Node<'_>) -> String {
        ensure_seams();
        let ctx = MemoryContext::new("parse-framed-roundtrip");
        let mcx = ctx.mcx();
        let text = nodeToString(mcx, node).expect("nodeToString");
        let parsed = string_to_node(mcx, text.as_str()).expect("string_to_node");
        let text2 = nodeToString(mcx, &parsed).expect("re-serialize");
        assert_eq!(text.as_str(), text2.as_str(), "framed re-serialize stable");
        text.as_str().to_string()
    }

    #[test]
    fn rangevar_round_trips() {
        ensure_seams();
        let ctx = MemoryContext::new("rv");
        let mcx = ctx.mcx();
        let rv = RangeVar {
            catalogname: None,
            schemaname: Some(PgString::from_str_in("public", mcx).unwrap()),
            relname: Some(PgString::from_str_in("t", mcx).unwrap()),
            inh: true,
            relpersistence: b'p' as i8,
            alias: None,
            location: 5,
        };
        let text = assert_framed_round_trip(&Node::mk_range_var(mcx, rv)?);
        assert!(text.starts_with("{RANGEVAR :catalogname <>"), "{text}");
        assert!(text.contains(":relname t"), "{text}");
        assert!(text.contains(":relpersistence p"), "{text}");
    }

    #[test]
    fn alias_round_trips() {
        ensure_seams();
        let ctx = MemoryContext::new("al");
        let mcx = ctx.mcx();
        let a = Alias {
            aliasname: Some(PgString::from_str_in("a", mcx).unwrap()),
            colnames: PgVec::new_in(mcx),
        };
        let text = assert_framed_round_trip(&Node::mk_alias(mcx, a)?);
        assert!(text.starts_with("{ALIAS :aliasname a :colnames <>"), "{text}");
    }

    #[test]
    fn sort_group_clause_round_trips() {
        let ctx = MemoryContext::new("sgc");
        let mcx = ctx.mcx();
        let s = SortGroupClause {
            tleSortGroupRef: 3,
            eqop: 96,
            sortop: 97,
            reverse_sort: true,
            nulls_first: false,
            hashable: true,
        };
        let text = assert_framed_round_trip(&Node::mk_sort_group_clause(mcx, s)?);
        assert!(text.starts_with("{SORTGROUPCLAUSE :tleSortGroupRef 3 :eqop 96"), "{text}");
        assert!(text.contains(":reverse_sort true"), "{text}");
        assert!(text.ends_with(":hashable true}"), "{text}");
    }

    #[test]
    fn rte_relation_round_trips() {
        ensure_seams();
        let ctx = MemoryContext::new("rte");
        let mcx = ctx.mcx();
        let mut r = RangeTblEntry::new_in(mcx);
        r.rtekind = RTEKind::RTE_RELATION;
        r.relid = 16384;
        r.inh = true;
        r.relkind = b'r' as i8;
        r.rellockmode = 1;
        r.perminfoindex = 1;
        r.lateral = false;
        r.inFromCl = true;
        let text = assert_framed_round_trip(&Node::mk_range_tbl_entry(mcx, r)?);
        assert!(text.starts_with("{RANGETBLENTRY :alias <> :eref <> :rtekind 0"), "{text}");
        assert!(text.contains(":relid 16384"), "{text}");
        assert!(text.contains(":relkind r"), "{text}");
        assert!(text.contains(":inFromCl true"), "{text}");
        // touch VarReturningType so the import is used (mirrors lib-test style).
        let _ = VarReturningType::VAR_RETURNING_DEFAULT;
    }

    #[test]
    fn common_table_expr_with_search_clause_round_trips() {
        // A CTE carrying a CTESearchClause (a typed struct, not a Node arm) and
        // scalar coltype/typmod/collation lists — exercises the framed
        // {CTESEARCHCLAUSE ...} sub-node round-trip (out + read) and the scalar
        // list fields, byte-stable across both.
        ensure_seams();
        let ctx = std::boxed::Box::leak(std::boxed::Box::new(MemoryContext::new("cte")));
        let mcx = ctx.mcx();
        let sc = CTESearchClause {
            search_col_list: PgVec::new_in(mcx),
            search_breadth_first: true,
            search_seq_column: Some(PgString::from_str_in("seq", mcx).unwrap()),
            location: -1,
        };
        let mut coltypes = PgVec::new_in(mcx);
        coltypes.push(23u32);
        let mut coltypmods = PgVec::new_in(mcx);
        coltypmods.push(-1i32);
        let mut colcollations = PgVec::new_in(mcx);
        colcollations.push(0u32);
        let cte = CommonTableExpr {
            ctename: Some(PgString::from_str_in("w", mcx).unwrap()),
            aliascolnames: PgVec::new_in(mcx),
            ctematerialized: CTEMaterialize::CTEMaterializeAlways,
            ctequery: None,
            search_clause: Some(mcx::alloc_in(mcx, sc).unwrap()),
            cycle_clause: None,
            location: -1,
            cterecursive: true,
            cterefcount: 1,
            ctecolnames: PgVec::new_in(mcx),
            ctecoltypes: coltypes,
            ctecoltypmods: coltypmods,
            ctecolcollations: colcollations,
        };
        let text = assert_framed_round_trip(&Node::mk_common_table_expr(mcx, cte)?);
        assert!(text.starts_with("{COMMONTABLEEXPR :ctename w"), "{text}");
        assert!(
            text.contains(":search_clause {CTESEARCHCLAUSE :search_col_list <> :search_breadth_first true :search_seq_column seq"),
            "{text}"
        );
        assert!(text.contains(":ctematerialized 1"), "{text}");
        assert!(text.contains(":cterecursive true"), "{text}");
        assert!(text.contains(":ctecoltypes (o 23)"), "{text}");

        // Read it back and confirm the search_clause survived.
        let parsed = string_to_node(mcx, &text).expect("read");
        {
            let __n = PgBox::into_inner(parsed);
            let __tag = __n.node_tag();
            match __n.into_commontableexpr() {
                Some(c) => {
                let sc = c.search_clause.expect("search_clause lost");
                assert!(sc.search_breadth_first);
                assert_eq!(sc.search_seq_column.as_ref().map(|s| s.as_str()), Some("seq"));
                assert_eq!(c.ctecoltypes.len(), 1);
                assert_eq!(c.ctecoltypes[0], 23);
                assert!(c.cterecursive);
            },
                None => panic!("expected CommonTableExpr, got {:?}", __tag),
            }
        }
    }
}
