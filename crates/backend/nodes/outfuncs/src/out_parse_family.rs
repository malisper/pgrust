//! `_out<Type>` writers for the out_parse_family node arms. Each writer mirrors its
//! `outfuncs.funcs.c` body field-for-field. `try_out` returns `true` iff it
//! claimed and wrote `node`.
//!
//! Covers the parse/query producer + raw-grammar node families: `Query`,
//! `RangeTblEntry` (custom, switch on rtekind), `RTEPermissionInfo`,
//! `RangeTblFunction`, `TableSampleClause`, `SortGroupClause`, `GroupingSet`,
//! `WindowClause`, `RowMarkClause`, `WithCheckOption`, `CTECycleClause`,
//! `SetOperationStmt`, `Alias`, `RangeVar`, `TypeName`, `ColumnDef`,
//! `RangeTblRef`, `JoinExpr`, `FromExpr`, `OnConflictExpr`, `MergeAction`,
//! `LockingClause`, `ColumnRef`, `ParamRef`, `A_Expr` (custom), `FuncCall`,
//! `A_Star`, `A_Indices`, `A_Indirection`, `A_ArrayExpr`, `ResTarget`,
//! `MultiAssignRef`, `TypeCast`, `CollateClause`, `SortBy`, `WindowDef`,
//! `RangeSubselect`, `RangeFunction`, `RangeTableSample`, `WithClause`,
//! `InferClause`, `OnConflictClause`, `MergeWhenClause`, `ReturningClause`,
//! `InsertStmt`, `DeleteStmt`, `UpdateStmt`, `MergeStmt`, `SelectStmt`,
//! `A_Const` (custom).
//!
//! `TableFunc` is fully serialized here (`_outTableFunc`, with its `plan` +
//! `location` fields and the XMLTABLE/JSON_TABLE Option-cell typed lists).
//!
//! Seam-panicked (carrier cannot round-trip):
//!   * `CommonTableExpr` — `search_clause` is `CTESearchClause`, which is NOT a
//!     `Node` enum variant, so it cannot be framed/round-tripped.

use alloc::string::String;

use core::fmt::Write as _;

use mcx::PgBox;
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::Expr;
use ::nodes::rawnodes::A_Expr_Kind;

use crate::{
    framed, out_expr, out_node_inner, write_bool_field, write_bitmapset_opt_field,
    write_char_field, write_enum_field, write_expr_field, write_float_field,
    write_int_field, write_int_list_field, write_location_field, write_oid_field,
    write_oid_list_field, write_string_field, write_uint64_field, write_uint_field,
};

// ---------------------------------------------------------------------------
// Local list/node-field helpers.
// ---------------------------------------------------------------------------

type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;

/// `WRITE_NODE_FIELD` over a `List *` of node pointers (`PgVec<NodePtr>`). C
/// `outNode` of a `List` renders the bare `(child child ...)` form, and a NULL
/// `List *` (NIL) renders `<>`. The owned model uses an empty `PgVec` for NIL,
/// so an empty vec → `<>`.
fn write_node_vec_field(buf: &mut String, name: &str, v: &[NodePtr<'_>], wl: bool) {
    let _ = write!(buf, " :{} ", name);
    if v.is_empty() {
        buf.push_str("<>");
        return;
    }
    buf.push('(');
    let mut first = true;
    for e in v {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_node_inner(buf, &**e, wl);
    }
    buf.push(')');
}

/// `WRITE_NODE_FIELD` over an `Oid` scalar list (`PgVec<Oid>`): `(o ...)` or
/// `<>` (NIL).
fn write_oid_vec_field(buf: &mut String, name: &str, v: &[u32]) {
    write_oid_list_field(buf, name, if v.is_empty() { None } else { Some(v) });
}

/// `WRITE_NODE_FIELD` over an `int` scalar list (`PgVec<i32>`): `(i ...)` or
/// `<>` (NIL).
fn write_int_vec_field(buf: &mut String, name: &str, v: &[i32]) {
    write_int_list_field(buf, name, if v.is_empty() { None } else { Some(v) });
}

/// `WRITE_NODE_FIELD` over an `Option<NodePtr>` (`Node *`): the child or `<>`.
fn write_opt_node_field(buf: &mut String, name: &str, n: &Option<NodePtr<'_>>, wl: bool) {
    let _ = write!(buf, " :{} ", name);
    match n {
        None => buf.push_str("<>"),
        Some(c) => out_node_inner(buf, &**c, wl),
    }
}

/// `WRITE_NODE_FIELD` over an optional framed child whose struct is NOT a
/// `Node` arm but whose body writer is named directly. Emits the `{LABEL ...}`
/// frame or `<>`.
fn write_opt_framed<T>(
    buf: &mut String,
    name: &str,
    n: &Option<PgBox<'_, T>>,
    wl: bool,
    body: impl Fn(&mut String, &T, bool),
) {
    let _ = write!(buf, " :{} ", name);
    match n {
        None => buf.push_str("<>"),
        Some(c) => framed(buf, |b| body(b, c, wl)),
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of direct-value structs (e.g.
/// `PgVec<RangeTblEntry>`), each emitted as a framed `{LABEL ...}` node. NIL
/// (empty) → `<>`.
fn write_value_vec_field<T>(
    buf: &mut String,
    name: &str,
    v: &[T],
    wl: bool,
    body: impl Fn(&mut String, &T, bool),
) {
    let _ = write!(buf, " :{} ", name);
    if v.is_empty() {
        buf.push_str("<>");
        return;
    }
    buf.push('(');
    let mut first = true;
    for e in v {
        if !first {
            buf.push(' ');
        }
        first = false;
        framed(buf, |b| body(b, e, wl));
    }
    buf.push(')');
}

/// `WRITE_NODE_FIELD` over a `List *` of `Expr` carried as `PgVec<PgBox<Expr>>`:
/// `({EXPR ...} ...)`, or `<>` for a NIL (`None`) list.
fn write_box_expr_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[PgBox<'_, Expr>]>,
    wl: bool,
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(v) => {
            buf.push('(');
            let mut first = true;
            for e in v {
                if !first {
                    buf.push(' ');
                }
                first = false;
                out_expr(buf, &**e, wl);
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `Expr` with NULL cells allowed, carried
/// as `PgVec<Option<PgBox<Expr>>>`: `({EXPR ...} <> ...)`, or `<>` for a NIL
/// (`None`) list.
fn write_opt_box_expr_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[Option<PgBox<'_, Expr>>]>,
    wl: bool,
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(v) => {
            buf.push('(');
            let mut first = true;
            for e in v {
                if !first {
                    buf.push(' ');
                }
                first = false;
                match e {
                    None => buf.push_str("<>"),
                    Some(b) => out_expr(buf, &**b, wl),
                }
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `String` value nodes carried as
/// `PgVec<PgString>` (`colnames`): `("a" "b" ...)`; NIL (`None`) → `<>`.
fn write_pgstring_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[mcx::PgString<'_>]>,
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(v) => {
            buf.push('(');
            let mut first = true;
            for s in v {
                if !first {
                    buf.push(' ');
                }
                first = false;
                buf.push('"');
                if !s.as_str().is_empty() {
                    crate::out_token(buf, s.as_str());
                }
                buf.push('"');
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `String` value nodes with NULL cells
/// allowed (`ns_names`, the DEFAULT namespace being a NULL `String *`), carried
/// as `PgVec<Option<PgString>>`: `("a" <> ...)`; NIL (`None`) → `<>`.
fn write_opt_pgstring_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[Option<mcx::PgString<'_>>]>,
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(v) => {
            buf.push('(');
            let mut first = true;
            for s in v {
                if !first {
                    buf.push(' ');
                }
                first = false;
                match s {
                    None => buf.push_str("<>"),
                    Some(s) => {
                        buf.push('"');
                        if !s.as_str().is_empty() {
                            crate::out_token(buf, s.as_str());
                        }
                        buf.push('"');
                    }
                }
            }
            buf.push(')');
        }
    }
}

/// `_outTableFunc` (outfuncs.funcs.c) — every field in struct order.
pub(crate) fn out_table_func(
    buf: &mut String,
    n: &::nodes::primnodes::TableFunc<'_>,
    wl: bool,
) {
    buf.push_str("TABLEFUNC");
    write_enum_field(buf, "functype", n.functype as i32);
    write_box_expr_list_field(buf, "ns_uris", n.ns_uris.as_deref(), wl);
    write_opt_pgstring_list_field(buf, "ns_names", n.ns_names.as_deref());
    write_expr_field(buf, "docexpr", n.docexpr.as_deref(), wl);
    write_expr_field(buf, "rowexpr", n.rowexpr.as_deref(), wl);
    write_pgstring_list_field(buf, "colnames", n.colnames.as_deref());
    write_oid_list_field(buf, "coltypes", n.coltypes.as_deref());
    write_int_list_field(buf, "coltypmods", n.coltypmods.as_deref());
    write_oid_list_field(buf, "colcollations", n.colcollations.as_deref());
    write_opt_box_expr_list_field(buf, "colexprs", n.colexprs.as_deref(), wl);
    write_opt_box_expr_list_field(buf, "coldefexprs", n.coldefexprs.as_deref(), wl);
    write_opt_box_expr_list_field(buf, "colvalexprs", n.colvalexprs.as_deref(), wl);
    write_box_expr_list_field(buf, "passingvalexprs", n.passingvalexprs.as_deref(), wl);
    write_bitmapset_opt_field(buf, "notnulls", n.notnulls.as_deref());
    write_opt_node_field(buf, "plan", &n.plan, wl);
    write_int_field(buf, "ordinalitycol", n.ordinalitycol);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outJsonTablePathScan` (outfuncs.funcs.c) — the `JsonTablePlan` leaf. The C
/// `path` is a `JsonTablePath` node (`Const *value; char *name;`); this port
/// collapses it into the `path` (the `Const` value node) + `name` fields, so we
/// serialize those two directly in place of the wrapper.
pub(crate) fn out_json_table_path_scan(
    buf: &mut String,
    n: &::nodes::primnodes::JsonTablePathScan<'_>,
    wl: bool,
) {
    buf.push_str("JSONTABLEPATHSCAN");
    crate::write_node_field(buf, "path", Some(&n.path), wl);
    crate::write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    crate::write_bool_field(buf, "errorOnError", n.errorOnError);
    crate::write_node_field(buf, "child", n.child.as_deref(), wl);
    crate::write_int_field(buf, "colMin", n.colMin);
    crate::write_int_field(buf, "colMax", n.colMax);
}

/// `_outJsonTableSiblingJoin` (outfuncs.funcs.c) — the sibling-join plan.
pub(crate) fn out_json_table_sibling_join(
    buf: &mut String,
    n: &::nodes::primnodes::JsonTableSiblingJoin<'_>,
    wl: bool,
) {
    buf.push_str("JSONTABLESIBLINGJOIN");
    crate::write_node_field(buf, "lplan", Some(&n.lplan), wl);
    crate::write_node_field(buf, "rplan", Some(&n.rplan), wl);
}

// ===========================================================================
// _outQuery
// ===========================================================================

pub(crate) fn out_query(buf: &mut String, n: &::nodes::copy_query::Query<'_>, wl: bool) {
    buf.push_str("QUERY");
    write_enum_field(buf, "commandType", n.commandType as i32);
    write_enum_field(buf, "querySource", n.querySource as i32);
    write_bool_field(buf, "canSetTag", n.canSetTag);
    write_opt_node_field(buf, "utilityStmt", &n.utilityStmt, wl);
    write_int_field(buf, "resultRelation", n.resultRelation);
    write_bool_field(buf, "hasAggs", n.hasAggs);
    write_bool_field(buf, "hasWindowFuncs", n.hasWindowFuncs);
    write_bool_field(buf, "hasTargetSRFs", n.hasTargetSRFs);
    write_bool_field(buf, "hasSubLinks", n.hasSubLinks);
    write_bool_field(buf, "hasDistinctOn", n.hasDistinctOn);
    write_bool_field(buf, "hasRecursive", n.hasRecursive);
    write_bool_field(buf, "hasModifyingCTE", n.hasModifyingCTE);
    write_bool_field(buf, "hasForUpdate", n.hasForUpdate);
    write_bool_field(buf, "hasRowSecurity", n.hasRowSecurity);
    write_bool_field(buf, "hasGroupRTE", n.hasGroupRTE);
    write_bool_field(buf, "isReturn", n.isReturn);
    write_node_vec_field(buf, "cteList", &n.cteList, wl);
    write_value_vec_field(buf, "rtable", &n.rtable, wl, out_range_tbl_entry);
    write_value_vec_field(buf, "rteperminfos", &n.rteperminfos, wl, out_rte_perm_info);
    write_opt_framed(buf, "jointree", &n.jointree, wl, out_from_expr);
    write_node_vec_field(buf, "mergeActionList", &n.mergeActionList, wl);
    write_int_field(buf, "mergeTargetRelation", n.mergeTargetRelation);
    write_expr_field(buf, "mergeJoinCondition", n.mergeJoinCondition.as_deref(), wl);
    write_value_vec_field(buf, "targetList", &n.targetList, wl, crate_out_targetentry);
    write_enum_field(buf, "override", n.r#override as i32);
    write_opt_framed(buf, "onConflict", &n.onConflict, wl, out_on_conflict_expr);
    write_string_field(buf, "returningOldAlias", n.returningOldAlias.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "returningNewAlias", n.returningNewAlias.as_ref().map(|s| s.as_str()));
    write_value_vec_field(buf, "returningList", &n.returningList, wl, crate_out_targetentry);
    write_node_vec_field(buf, "groupClause", &n.groupClause, wl);
    write_bool_field(buf, "groupDistinct", n.groupDistinct);
    write_node_vec_field(buf, "groupingSets", &n.groupingSets, wl);
    write_expr_field(buf, "havingQual", n.havingQual.as_deref(), wl);
    write_node_vec_field(buf, "windowClause", &n.windowClause, wl);
    write_node_vec_field(buf, "distinctClause", &n.distinctClause, wl);
    write_node_vec_field(buf, "sortClause", &n.sortClause, wl);
    write_expr_field(buf, "limitOffset", n.limitOffset.as_deref(), wl);
    write_expr_field(buf, "limitCount", n.limitCount.as_deref(), wl);
    write_enum_field(buf, "limitOption", n.limitOption as i32);
    write_node_vec_field(buf, "rowMarks", &n.rowMarks, wl);
    write_opt_node_field(buf, "setOperations", &n.setOperations, wl);
    write_oid_vec_field(buf, "constraintDeps", &n.constraintDeps);
    write_node_vec_field(buf, "withCheckOptions", &n.withCheckOptions, wl);
    write_location_field(buf, "stmt_location", n.stmt_location, wl);
    write_location_field(buf, "stmt_len", n.stmt_len, wl);
}

/// `_outTargetEntry` lives in the lib; reuse it by re-deriving the same body.
/// (The lib's `out_targetentry` is private, so this delegates through the
/// `Node::TargetEntry` arm of `out_node_inner` by framing it directly.)
fn crate_out_targetentry(buf: &mut String, te: &::nodes::primnodes::TargetEntry<'_>, wl: bool) {
    // The lib owns _outTargetEntry; route a direct-value TargetEntry through
    // out_node_inner by constructing the framing here. out_node_inner frames
    // Node::TargetEntry itself, so we instead replicate the dispatch by calling
    // it on a borrowed Node. We can't build a Node without moving; the lib's
    // out_node_inner takes &Node. Emit the body via the lib's public framing of
    // the value through a transient is not possible, so mirror the lib writer.
    buf.push_str("TARGETENTRY");
    buf.push_str(" :expr ");
    match te.expr.as_deref() {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, wl),
    }
    write_int_field(buf, "resno", te.resno as i32);
    write_string_field(buf, "resname", te.resname.as_ref().map(|s| s.as_str()));
    write_uint_field(buf, "ressortgroupref", te.ressortgroupref);
    write_oid_field(buf, "resorigtbl", te.resorigtbl);
    write_int_field(buf, "resorigcol", te.resorigcol as i32);
    write_bool_field(buf, "resjunk", te.resjunk);
}

// ===========================================================================
// _outRangeTblEntry — custom, switch on rtekind.
// ===========================================================================

pub(crate) fn out_range_tbl_entry(buf: &mut String, n: &::nodes::parsenodes::RangeTblEntry<'_>, wl: bool) {
    use ::nodes::parsenodes::RTEKind;
    buf.push_str("RANGETBLENTRY");
    write_opt_framed(buf, "alias", &n.alias, wl, out_alias);
    write_opt_framed(buf, "eref", &n.eref, wl, out_alias);
    write_enum_field(buf, "rtekind", n.rtekind as i32);

    match n.rtekind {
        RTEKind::RTE_RELATION => {
            write_oid_field(buf, "relid", n.relid);
            write_bool_field(buf, "inh", n.inh);
            write_char_field(buf, "relkind", n.relkind as u8);
            write_int_field(buf, "rellockmode", n.rellockmode);
            write_uint_field(buf, "perminfoindex", n.perminfoindex);
            write_opt_node_field(buf, "tablesample", &n.tablesample, wl);
        }
        RTEKind::RTE_SUBQUERY => {
            write_opt_framed(buf, "subquery", &n.subquery, wl, out_query);
            write_bool_field(buf, "security_barrier", n.security_barrier);
            write_oid_field(buf, "relid", n.relid);
            write_bool_field(buf, "inh", n.inh);
            write_char_field(buf, "relkind", n.relkind as u8);
            write_int_field(buf, "rellockmode", n.rellockmode);
            write_uint_field(buf, "perminfoindex", n.perminfoindex);
        }
        RTEKind::RTE_JOIN => {
            write_enum_field(buf, "jointype", n.jointype as i32);
            write_int_field(buf, "joinmergedcols", n.joinmergedcols);
            write_node_vec_field(buf, "joinaliasvars", &n.joinaliasvars, wl);
            write_int_vec_field(buf, "joinleftcols", &n.joinleftcols);
            write_int_vec_field(buf, "joinrightcols", &n.joinrightcols);
            write_opt_framed(buf, "join_using_alias", &n.join_using_alias, wl, out_alias);
        }
        RTEKind::RTE_FUNCTION => {
            write_node_vec_field(buf, "functions", &n.functions, wl);
            write_bool_field(buf, "funcordinality", n.funcordinality);
        }
        RTEKind::RTE_TABLEFUNC => {
            write_opt_node_field(buf, "tablefunc", &n.tablefunc, wl);
        }
        RTEKind::RTE_VALUES => {
            write_node_vec_field(buf, "values_lists", &n.values_lists, wl);
            write_oid_vec_field(buf, "coltypes", &n.coltypes);
            write_int_vec_field(buf, "coltypmods", &n.coltypmods);
            write_oid_vec_field(buf, "colcollations", &n.colcollations);
        }
        RTEKind::RTE_CTE => {
            write_string_field(buf, "ctename", n.ctename.as_ref().map(|s| s.as_str()));
            write_uint_field(buf, "ctelevelsup", n.ctelevelsup);
            write_bool_field(buf, "self_reference", n.self_reference);
            write_oid_vec_field(buf, "coltypes", &n.coltypes);
            write_int_vec_field(buf, "coltypmods", &n.coltypmods);
            write_oid_vec_field(buf, "colcollations", &n.colcollations);
        }
        RTEKind::RTE_NAMEDTUPLESTORE => {
            write_string_field(buf, "enrname", n.enrname.as_ref().map(|s| s.as_str()));
            write_float_field(buf, "enrtuples", n.enrtuples);
            write_oid_vec_field(buf, "coltypes", &n.coltypes);
            write_int_vec_field(buf, "coltypmods", &n.coltypmods);
            write_oid_vec_field(buf, "colcollations", &n.colcollations);
            write_oid_field(buf, "relid", n.relid);
        }
        RTEKind::RTE_RESULT => {
            // nothing
        }
        RTEKind::RTE_GROUP => {
            write_node_vec_field(buf, "groupexprs", &n.groupexprs, wl);
        }
    }

    write_bool_field(buf, "lateral", n.lateral);
    write_bool_field(buf, "inFromCl", n.inFromCl);
    write_node_vec_field(buf, "securityQuals", &n.securityQuals, wl);
}

// ===========================================================================
// _outRTEPermissionInfo
// ===========================================================================

pub(crate) fn out_rte_perm_info(buf: &mut String, n: &::nodes::parsenodes::RTEPermissionInfo<'_>, _wl: bool) {
    buf.push_str("RTEPERMISSIONINFO");
    write_oid_field(buf, "relid", n.relid);
    write_bool_field(buf, "inh", n.inh);
    write_uint64_field(buf, "requiredPerms", n.requiredPerms as u64);
    write_oid_field(buf, "checkAsUser", n.checkAsUser);
    write_bitmapset_opt_field(buf, "selectedCols", n.selectedCols.as_deref());
    write_bitmapset_opt_field(buf, "insertedCols", n.insertedCols.as_deref());
    write_bitmapset_opt_field(buf, "updatedCols", n.updatedCols.as_deref());
}

// ===========================================================================
// _outRangeTblFunction
// ===========================================================================

pub(crate) fn out_range_tbl_function(
    buf: &mut String,
    n: &::nodes::rawnodes::RangeTblFunction<'_>,
    wl: bool,
) {
    buf.push_str("RANGETBLFUNCTION");
    write_opt_node_field(buf, "funcexpr", &n.funcexpr, wl);
    write_int_field(buf, "funccolcount", n.funccolcount);
    write_node_vec_field(buf, "funccolnames", &n.funccolnames, wl);
    write_oid_vec_field(buf, "funccoltypes", &n.funccoltypes);
    write_int_vec_field(buf, "funccoltypmods", &n.funccoltypmods);
    write_oid_vec_field(buf, "funccolcollations", &n.funccolcollations);
    write_bitmapset_opt_field(buf, "funcparams", n.funcparams.as_deref());
}

// ===========================================================================
// _outTableSampleClause
// ===========================================================================

pub(crate) fn out_table_sample_clause(buf: &mut String, n: &::nodes::nodesamplescan::TableSampleClause<'_>, wl: bool) {
    buf.push_str("TABLESAMPLECLAUSE");
    write_oid_field(buf, "tsmhandler", n.tsmhandler);
    // args: Option<PgVec<Expr>>. C WRITE_NODE_FIELD of a List of Expr → bare
    // `(expr ...)` form; NIL/None/empty → `<>`.
    let _ = write!(buf, " :args ");
    match &n.args {
        Some(list) if !list.is_empty() => write_expr_list_tail(buf, list.as_slice(), wl),
        _ => buf.push_str("<>"),
    }
    write_expr_field(buf, "repeatable", n.repeatable.as_deref(), wl);
}

/// Emit `(expr expr ...)` for a non-empty Expr slice (the C `_outList`/`outNode`
/// bare list form).
fn write_expr_list_tail(buf: &mut String, args: &[Expr], wl: bool) {
    buf.push('(');
    let mut first = true;
    for a in args {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_expr(buf, a, wl);
    }
    buf.push(')');
}

// ===========================================================================
// _outSortGroupClause
// ===========================================================================

pub(crate) fn out_sort_group_clause(
    buf: &mut String,
    n: &::nodes::rawnodes::SortGroupClause,
    _wl: bool,
) {
    buf.push_str("SORTGROUPCLAUSE");
    write_uint_field(buf, "tleSortGroupRef", n.tleSortGroupRef);
    write_oid_field(buf, "eqop", n.eqop);
    write_oid_field(buf, "sortop", n.sortop);
    write_bool_field(buf, "reverse_sort", n.reverse_sort);
    write_bool_field(buf, "nulls_first", n.nulls_first);
    write_bool_field(buf, "hashable", n.hashable);
}

// ===========================================================================
// _outGroupingSet
// ===========================================================================

fn out_grouping_set(buf: &mut String, n: &::nodes::rawnodes::GroupingSet<'_>, wl: bool) {
    buf.push_str("GROUPINGSET");
    write_enum_field(buf, "kind", n.kind as i32);
    write_node_vec_field(buf, "content", &n.content, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outWindowClause
// ===========================================================================

fn out_window_clause(buf: &mut String, n: &::nodes::rawnodes::WindowClause<'_>, wl: bool) {
    buf.push_str("WINDOWCLAUSE");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "refname", n.refname.as_ref().map(|s| s.as_str()));
    write_node_vec_field(buf, "partitionClause", &n.partitionClause, wl);
    write_node_vec_field(buf, "orderClause", &n.orderClause, wl);
    write_int_field(buf, "frameOptions", n.frameOptions);
    write_opt_node_field(buf, "startOffset", &n.startOffset, wl);
    write_opt_node_field(buf, "endOffset", &n.endOffset, wl);
    write_oid_field(buf, "startInRangeFunc", n.startInRangeFunc);
    write_oid_field(buf, "endInRangeFunc", n.endInRangeFunc);
    write_oid_field(buf, "inRangeColl", n.inRangeColl);
    write_bool_field(buf, "inRangeAsc", n.inRangeAsc);
    write_bool_field(buf, "inRangeNullsFirst", n.inRangeNullsFirst);
    write_uint_field(buf, "winref", n.winref);
    write_bool_field(buf, "copiedOrder", n.copiedOrder);
}

// ===========================================================================
// _outRowMarkClause
// ===========================================================================

fn out_row_mark_clause(buf: &mut String, n: &::nodes::rawnodes::RowMarkClause, _wl: bool) {
    buf.push_str("ROWMARKCLAUSE");
    write_uint_field(buf, "rti", n.rti);
    write_enum_field(buf, "strength", n.strength as i32);
    write_enum_field(buf, "waitPolicy", n.waitPolicy as i32);
    write_bool_field(buf, "pushedDown", n.pushedDown);
}

// ===========================================================================
// _outWithCheckOption
// ===========================================================================

fn out_with_check_option(buf: &mut String, n: &::nodes::rawnodes::WithCheckOption<'_>, wl: bool) {
    buf.push_str("WITHCHECKOPTION");
    write_enum_field(buf, "kind", n.kind as i32);
    write_string_field(buf, "relname", n.relname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "polname", n.polname.as_ref().map(|s| s.as_str()));
    write_opt_node_field(buf, "qual", &n.qual, wl);
    write_bool_field(buf, "cascaded", n.cascaded);
}

// ===========================================================================
// _outCTECycleClause
// ===========================================================================

fn out_cte_cycle_clause(buf: &mut String, n: &::nodes::rawnodes::CTECycleClause<'_>, wl: bool) {
    buf.push_str("CTECYCLECLAUSE");
    write_node_vec_field(buf, "cycle_col_list", &n.cycle_col_list, wl);
    write_string_field(buf, "cycle_mark_column", n.cycle_mark_column.as_ref().map(|s| s.as_str()));
    write_opt_node_field(buf, "cycle_mark_value", &n.cycle_mark_value, wl);
    write_opt_node_field(buf, "cycle_mark_default", &n.cycle_mark_default, wl);
    write_string_field(buf, "cycle_path_column", n.cycle_path_column.as_ref().map(|s| s.as_str()));
    write_location_field(buf, "location", n.location, wl);
    write_oid_field(buf, "cycle_mark_type", n.cycle_mark_type);
    write_int_field(buf, "cycle_mark_typmod", n.cycle_mark_typmod);
    write_oid_field(buf, "cycle_mark_collation", n.cycle_mark_collation);
    write_oid_field(buf, "cycle_mark_neop", n.cycle_mark_neop);
}

/// `_outCTESearchClause` (outfuncs.funcs.c) — the framed `{CTESEARCHCLAUSE ...}`
/// body. `CTESearchClause` is a typed struct (not a `Node` enum arm), so it is
/// serialized directly here and emitted via [`write_opt_framed`] from the
/// parent `_outCommonTableExpr`; this is byte-identical to C's
/// `WRITE_NODE_FIELD(search_clause)`.
fn out_cte_search_clause(buf: &mut String, n: &::nodes::rawnodes::CTESearchClause<'_>, wl: bool) {
    buf.push_str("CTESEARCHCLAUSE");
    write_node_vec_field(buf, "search_col_list", &n.search_col_list, wl);
    write_bool_field(buf, "search_breadth_first", n.search_breadth_first);
    write_string_field(buf, "search_seq_column", n.search_seq_column.as_ref().map(|s| s.as_str()));
    write_location_field(buf, "location", n.location, wl);
}

/// `_outCommonTableExpr` (outfuncs.funcs.c).
fn out_common_table_expr(buf: &mut String, n: &::nodes::rawnodes::CommonTableExpr<'_>, wl: bool) {
    buf.push_str("COMMONTABLEEXPR");
    write_string_field(buf, "ctename", n.ctename.as_ref().map(|s| s.as_str()));
    write_node_vec_field(buf, "aliascolnames", &n.aliascolnames, wl);
    write_enum_field(buf, "ctematerialized", n.ctematerialized as i32);
    write_opt_node_field(buf, "ctequery", &n.ctequery, wl);
    // search_clause: a CTESearchClause (typed struct, not a `Node` arm) — framed
    // directly. cycle_clause: a `Node *` (`Node::CTECycleClause`).
    write_opt_framed(buf, "search_clause", &n.search_clause, wl, out_cte_search_clause);
    write_opt_node_field(buf, "cycle_clause", &n.cycle_clause, wl);
    write_location_field(buf, "location", n.location, wl);
    write_bool_field(buf, "cterecursive", n.cterecursive);
    write_int_field(buf, "cterefcount", n.cterefcount);
    write_node_vec_field(buf, "ctecolnames", &n.ctecolnames, wl);
    write_oid_vec_field(buf, "ctecoltypes", &n.ctecoltypes);
    write_int_vec_field(buf, "ctecoltypmods", &n.ctecoltypmods);
    write_oid_vec_field(buf, "ctecolcollations", &n.ctecolcollations);
}

// ===========================================================================
// _outSetOperationStmt
// ===========================================================================

fn out_set_operation_stmt(buf: &mut String, n: &::nodes::rawnodes::SetOperationStmt<'_>, wl: bool) {
    buf.push_str("SETOPERATIONSTMT");
    write_enum_field(buf, "op", n.op as i32);
    write_bool_field(buf, "all", n.all);
    write_opt_node_field(buf, "larg", &n.larg, wl);
    write_opt_node_field(buf, "rarg", &n.rarg, wl);
    write_oid_vec_field(buf, "colTypes", &n.colTypes);
    write_int_vec_field(buf, "colTypmods", &n.colTypmods);
    write_oid_vec_field(buf, "colCollations", &n.colCollations);
    write_node_vec_field(buf, "groupClauses", &n.groupClauses, wl);
}

// ===========================================================================
// _outAlias
// ===========================================================================

fn out_alias(buf: &mut String, n: &::nodes::rawnodes::Alias<'_>, wl: bool) {
    buf.push_str("ALIAS");
    write_string_field(buf, "aliasname", n.aliasname.as_ref().map(|s| s.as_str()));
    write_node_vec_field(buf, "colnames", &n.colnames, wl);
}

// ===========================================================================
// _outRangeVar
// ===========================================================================

fn out_range_var(buf: &mut String, n: &::nodes::rawnodes::RangeVar<'_>, wl: bool) {
    buf.push_str("RANGEVAR");
    write_string_field(buf, "catalogname", n.catalogname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "schemaname", n.schemaname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "relname", n.relname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "inh", n.inh);
    write_char_field(buf, "relpersistence", n.relpersistence as u8);
    write_opt_framed(buf, "alias", &n.alias, wl, out_alias);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outTypeName
// ===========================================================================

fn out_type_name(buf: &mut String, n: &::nodes::rawnodes::TypeName<'_>, wl: bool) {
    buf.push_str("TYPENAME");
    write_node_vec_field(buf, "names", &n.names, wl);
    write_oid_field(buf, "typeOid", n.typeOid);
    write_bool_field(buf, "setof", n.setof);
    write_bool_field(buf, "pct_type", n.pct_type);
    write_node_vec_field(buf, "typmods", &n.typmods, wl);
    write_int_field(buf, "typemod", n.typemod);
    write_node_vec_field(buf, "arrayBounds", &n.arrayBounds, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outColumnDef
// ===========================================================================

fn out_column_def(buf: &mut String, n: &::nodes::rawnodes::ColumnDef<'_>, wl: bool) {
    buf.push_str("COLUMNDEF");
    write_string_field(buf, "colname", n.colname.as_ref().map(|s| s.as_str()));
    write_opt_framed(buf, "typeName", &n.typeName, wl, out_type_name);
    write_string_field(buf, "compression", n.compression.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "inhcount", n.inhcount as i32);
    write_bool_field(buf, "is_local", n.is_local);
    write_bool_field(buf, "is_not_null", n.is_not_null);
    write_bool_field(buf, "is_from_type", n.is_from_type);
    write_char_field(buf, "storage", n.storage as u8);
    write_string_field(buf, "storage_name", n.storage_name.as_ref().map(|s| s.as_str()));
    write_opt_node_field(buf, "raw_default", &n.raw_default, wl);
    write_opt_node_field(buf, "cooked_default", &n.cooked_default, wl);
    write_char_field(buf, "identity", n.identity as u8);
    write_opt_framed(buf, "identitySequence", &n.identitySequence, wl, out_range_var);
    write_char_field(buf, "generated", n.generated as u8);
    write_opt_framed(buf, "collClause", &n.collClause, wl, out_collate_clause);
    write_oid_field(buf, "collOid", n.collOid);
    write_node_vec_field(buf, "constraints", &n.constraints, wl);
    write_node_vec_field(buf, "fdwoptions", &n.fdwoptions, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outRangeTblRef
// ===========================================================================

fn out_range_tbl_ref(buf: &mut String, n: &::nodes::rawnodes::RangeTblRef, _wl: bool) {
    buf.push_str("RANGETBLREF");
    write_int_field(buf, "rtindex", n.rtindex);
}

// ===========================================================================
// _outJoinExpr
// ===========================================================================

fn out_join_expr(buf: &mut String, n: &::nodes::rawnodes::JoinExpr<'_>, wl: bool) {
    buf.push_str("JOINEXPR");
    write_enum_field(buf, "jointype", n.jointype as i32);
    write_bool_field(buf, "isNatural", n.isNatural);
    write_opt_node_field(buf, "larg", &n.larg, wl);
    write_opt_node_field(buf, "rarg", &n.rarg, wl);
    write_node_vec_field(buf, "usingClause", &n.usingClause, wl);
    write_opt_framed(buf, "join_using_alias", &n.join_using_alias, wl, out_alias);
    write_opt_node_field(buf, "quals", &n.quals, wl);
    write_opt_framed(buf, "alias", &n.alias, wl, out_alias);
    write_int_field(buf, "rtindex", n.rtindex);
}

// ===========================================================================
// _outFromExpr
// ===========================================================================

fn out_from_expr(buf: &mut String, n: &::nodes::rawnodes::FromExpr<'_>, wl: bool) {
    buf.push_str("FROMEXPR");
    write_node_vec_field(buf, "fromlist", &n.fromlist, wl);
    write_opt_node_field(buf, "quals", &n.quals, wl);
}

// ===========================================================================
// _outOnConflictExpr
// ===========================================================================

fn out_on_conflict_expr(buf: &mut String, n: &::nodes::rawnodes::OnConflictExpr<'_>, wl: bool) {
    buf.push_str("ONCONFLICTEXPR");
    write_enum_field(buf, "action", n.action as i32);
    write_node_vec_field(buf, "arbiterElems", &n.arbiterElems, wl);
    write_opt_node_field(buf, "arbiterWhere", &n.arbiterWhere, wl);
    write_oid_field(buf, "constraint", n.constraint);
    write_node_vec_field(buf, "onConflictSet", &n.onConflictSet, wl);
    write_opt_node_field(buf, "onConflictWhere", &n.onConflictWhere, wl);
    write_int_field(buf, "exclRelIndex", n.exclRelIndex);
    write_node_vec_field(buf, "exclRelTlist", &n.exclRelTlist, wl);
}

// ===========================================================================
// _outMergeAction
// ===========================================================================

fn out_merge_action(buf: &mut String, n: &::nodes::rawnodes::MergeAction<'_>, wl: bool) {
    buf.push_str("MERGEACTION");
    write_enum_field(buf, "matchKind", n.matchKind as i32);
    write_enum_field(buf, "commandType", n.commandType as i32);
    write_enum_field(buf, "override", n.r#override as i32);
    write_opt_node_field(buf, "qual", &n.qual, wl);
    write_node_vec_field(buf, "targetList", &n.targetList, wl);
    write_int_vec_field(buf, "updateColnos", &n.updateColnos);
}

// ===========================================================================
// _outLockingClause
// ===========================================================================

fn out_locking_clause(buf: &mut String, n: &::nodes::rawnodes::LockingClause<'_>, wl: bool) {
    buf.push_str("LOCKINGCLAUSE");
    write_node_vec_field(buf, "lockedRels", &n.lockedRels, wl);
    write_enum_field(buf, "strength", n.strength as i32);
    write_enum_field(buf, "waitPolicy", n.waitPolicy as i32);
}

// ===========================================================================
// _outColumnRef
// ===========================================================================

fn out_column_ref(buf: &mut String, n: &::nodes::rawnodes::ColumnRef<'_>, wl: bool) {
    buf.push_str("COLUMNREF");
    write_node_vec_field(buf, "fields", &n.fields, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outParamRef
// ===========================================================================

fn out_param_ref(buf: &mut String, n: &::nodes::rawnodes::ParamRef, wl: bool) {
    buf.push_str("PARAMREF");
    write_int_field(buf, "number", n.number);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outA_Expr — custom, switch on kind.
// ===========================================================================

fn out_a_expr(buf: &mut String, n: &::nodes::rawnodes::A_Expr<'_>, wl: bool) {
    buf.push_str("A_EXPR");
    match n.kind {
        A_Expr_Kind::AEXPR_OP => {
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_OP_ANY => {
            buf.push_str(" ANY");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_OP_ALL => {
            buf.push_str(" ALL");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_DISTINCT => {
            buf.push_str(" DISTINCT");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_NOT_DISTINCT => {
            buf.push_str(" NOT_DISTINCT");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_NULLIF => {
            buf.push_str(" NULLIF");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_IN => {
            buf.push_str(" IN");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_LIKE => {
            buf.push_str(" LIKE");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_ILIKE => {
            buf.push_str(" ILIKE");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_SIMILAR => {
            buf.push_str(" SIMILAR");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_BETWEEN => {
            buf.push_str(" BETWEEN");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN => {
            buf.push_str(" NOT_BETWEEN");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_BETWEEN_SYM => {
            buf.push_str(" BETWEEN_SYM");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
        A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
            buf.push_str(" NOT_BETWEEN_SYM");
            write_node_vec_field(buf, "name", &n.name, wl);
        }
    }
    write_opt_node_field(buf, "lexpr", &n.lexpr, wl);
    write_opt_node_field(buf, "rexpr", &n.rexpr, wl);
    write_location_field(buf, "rexpr_list_start", n.rexpr_list_start, wl);
    write_location_field(buf, "rexpr_list_end", n.rexpr_list_end, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outFuncCall
// ===========================================================================

fn out_func_call(buf: &mut String, n: &::nodes::rawnodes::FuncCall<'_>, wl: bool) {
    buf.push_str("FUNCCALL");
    write_node_vec_field(buf, "funcname", &n.funcname, wl);
    write_node_vec_field(buf, "args", &n.args, wl);
    write_node_vec_field(buf, "agg_order", &n.agg_order, wl);
    write_opt_node_field(buf, "agg_filter", &n.agg_filter, wl);
    write_opt_framed(buf, "over", &n.over, wl, out_window_def);
    write_bool_field(buf, "agg_within_group", n.agg_within_group);
    write_bool_field(buf, "agg_star", n.agg_star);
    write_bool_field(buf, "agg_distinct", n.agg_distinct);
    write_bool_field(buf, "func_variadic", n.func_variadic);
    write_enum_field(buf, "funcformat", n.funcformat as i32);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outA_Star / _outA_Indices / _outA_Indirection / _outA_ArrayExpr
// ===========================================================================

fn out_a_star(buf: &mut String, _n: &::nodes::rawnodes::A_Star, _wl: bool) {
    buf.push_str("A_STAR");
}

fn out_a_indices(buf: &mut String, n: &::nodes::rawnodes::A_Indices<'_>, wl: bool) {
    buf.push_str("A_INDICES");
    write_bool_field(buf, "is_slice", n.is_slice);
    write_opt_node_field(buf, "lidx", &n.lidx, wl);
    write_opt_node_field(buf, "uidx", &n.uidx, wl);
}

fn out_a_indirection(buf: &mut String, n: &::nodes::rawnodes::A_Indirection<'_>, wl: bool) {
    buf.push_str("A_INDIRECTION");
    write_opt_node_field(buf, "arg", &n.arg, wl);
    write_node_vec_field(buf, "indirection", &n.indirection, wl);
}

fn out_a_array_expr(buf: &mut String, n: &::nodes::rawnodes::A_ArrayExpr<'_>, wl: bool) {
    buf.push_str("A_ARRAYEXPR");
    write_node_vec_field(buf, "elements", &n.elements, wl);
    write_location_field(buf, "list_start", n.list_start, wl);
    write_location_field(buf, "list_end", n.list_end, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outResTarget / _outMultiAssignRef / _outTypeCast / _outCollateClause
// ===========================================================================

fn out_res_target(buf: &mut String, n: &::nodes::rawnodes::ResTarget<'_>, wl: bool) {
    buf.push_str("RESTARGET");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_vec_field(buf, "indirection", &n.indirection, wl);
    write_opt_node_field(buf, "val", &n.val, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_multi_assign_ref(buf: &mut String, n: &::nodes::rawnodes::MultiAssignRef<'_>, wl: bool) {
    buf.push_str("MULTIASSIGNREF");
    write_opt_node_field(buf, "source", &n.source, wl);
    write_int_field(buf, "colno", n.colno);
    write_int_field(buf, "ncolumns", n.ncolumns);
}

fn out_type_cast(buf: &mut String, n: &::nodes::rawnodes::TypeCast<'_>, wl: bool) {
    buf.push_str("TYPECAST");
    write_opt_node_field(buf, "arg", &n.arg, wl);
    write_opt_framed(buf, "typeName", &n.typeName, wl, out_type_name);
    write_location_field(buf, "location", n.location, wl);
}

fn out_collate_clause(buf: &mut String, n: &::nodes::rawnodes::CollateClause<'_>, wl: bool) {
    buf.push_str("COLLATECLAUSE");
    write_opt_node_field(buf, "arg", &n.arg, wl);
    write_node_vec_field(buf, "collname", &n.collname, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outSortBy / _outWindowDef
// ===========================================================================

fn out_sort_by(buf: &mut String, n: &::nodes::rawnodes::SortBy<'_>, wl: bool) {
    buf.push_str("SORTBY");
    write_opt_node_field(buf, "node", &n.node, wl);
    write_enum_field(buf, "sortby_dir", n.sortby_dir as i32);
    write_enum_field(buf, "sortby_nulls", n.sortby_nulls as i32);
    write_node_vec_field(buf, "useOp", &n.useOp, wl);
    write_location_field(buf, "location", n.location, wl);
}

fn out_window_def(buf: &mut String, n: &::nodes::rawnodes::WindowDef<'_>, wl: bool) {
    buf.push_str("WINDOWDEF");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "refname", n.refname.as_ref().map(|s| s.as_str()));
    write_node_vec_field(buf, "partitionClause", &n.partitionClause, wl);
    write_node_vec_field(buf, "orderClause", &n.orderClause, wl);
    write_int_field(buf, "frameOptions", n.frameOptions);
    write_opt_node_field(buf, "startOffset", &n.startOffset, wl);
    write_opt_node_field(buf, "endOffset", &n.endOffset, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outRangeSubselect / _outRangeFunction / _outRangeTableSample
// ===========================================================================

fn out_range_subselect(buf: &mut String, n: &::nodes::rawnodes::RangeSubselect<'_>, wl: bool) {
    buf.push_str("RANGESUBSELECT");
    write_bool_field(buf, "lateral", n.lateral);
    write_opt_node_field(buf, "subquery", &n.subquery, wl);
    write_opt_framed(buf, "alias", &n.alias, wl, out_alias);
}

fn out_range_function(buf: &mut String, n: &::nodes::rawnodes::RangeFunction<'_>, wl: bool) {
    buf.push_str("RANGEFUNCTION");
    write_bool_field(buf, "lateral", n.lateral);
    write_bool_field(buf, "ordinality", n.ordinality);
    write_bool_field(buf, "is_rowsfrom", n.is_rowsfrom);
    write_node_vec_field(buf, "functions", &n.functions, wl);
    write_opt_framed(buf, "alias", &n.alias, wl, out_alias);
    write_node_vec_field(buf, "coldeflist", &n.coldeflist, wl);
}

fn out_range_table_sample(buf: &mut String, n: &::nodes::rawnodes::RangeTableSample<'_>, wl: bool) {
    buf.push_str("RANGETABLESAMPLE");
    write_opt_node_field(buf, "relation", &n.relation, wl);
    write_node_vec_field(buf, "method", &n.method, wl);
    write_node_vec_field(buf, "args", &n.args, wl);
    write_opt_node_field(buf, "repeatable", &n.repeatable, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outWithClause / _outInferClause / _outOnConflictClause
// ===========================================================================

fn out_with_clause(buf: &mut String, n: &::nodes::rawnodes::WithClause<'_>, wl: bool) {
    buf.push_str("WITHCLAUSE");
    write_node_vec_field(buf, "ctes", &n.ctes, wl);
    write_bool_field(buf, "recursive", n.recursive);
    write_location_field(buf, "location", n.location, wl);
}

fn out_infer_clause(buf: &mut String, n: &::nodes::rawnodes::InferClause<'_>, wl: bool) {
    buf.push_str("INFERCLAUSE");
    write_node_vec_field(buf, "indexElems", &n.indexElems, wl);
    write_opt_node_field(buf, "whereClause", &n.whereClause, wl);
    write_string_field(buf, "conname", n.conname.as_ref().map(|s| s.as_str()));
    write_location_field(buf, "location", n.location, wl);
}

fn out_on_conflict_clause(buf: &mut String, n: &::nodes::rawnodes::OnConflictClause<'_>, wl: bool) {
    buf.push_str("ONCONFLICTCLAUSE");
    write_enum_field(buf, "action", n.action as i32);
    write_opt_framed(buf, "infer", &n.infer, wl, out_infer_clause);
    write_node_vec_field(buf, "targetList", &n.targetList, wl);
    write_opt_node_field(buf, "whereClause", &n.whereClause, wl);
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// _outMergeWhenClause / _outReturningClause
// ===========================================================================

fn out_merge_when_clause(buf: &mut String, n: &::nodes::rawnodes::MergeWhenClause<'_>, wl: bool) {
    buf.push_str("MERGEWHENCLAUSE");
    write_enum_field(buf, "matchKind", n.matchKind as i32);
    write_enum_field(buf, "commandType", n.commandType as i32);
    write_enum_field(buf, "override", n.r#override as i32);
    write_opt_node_field(buf, "condition", &n.condition, wl);
    write_node_vec_field(buf, "targetList", &n.targetList, wl);
    write_node_vec_field(buf, "values", &n.values, wl);
}

fn out_returning_clause(buf: &mut String, n: &::nodes::rawnodes::ReturningClause<'_>, wl: bool) {
    buf.push_str("RETURNINGCLAUSE");
    write_node_vec_field(buf, "options", &n.options, wl);
    write_node_vec_field(buf, "exprs", &n.exprs, wl);
}

// ===========================================================================
// _outInsertStmt / _outDeleteStmt / _outUpdateStmt / _outMergeStmt
// ===========================================================================

fn out_insert_stmt(buf: &mut String, n: &::nodes::rawnodes::InsertStmt<'_>, wl: bool) {
    buf.push_str("INSERTSTMT");
    write_opt_framed(buf, "relation", &n.relation, wl, out_range_var);
    write_node_vec_field(buf, "cols", &n.cols, wl);
    write_opt_node_field(buf, "selectStmt", &n.selectStmt, wl);
    write_opt_framed(buf, "onConflictClause", &n.onConflictClause, wl, out_on_conflict_clause);
    write_opt_framed(buf, "returningClause", &n.returningClause, wl, out_returning_clause);
    write_opt_framed(buf, "withClause", &n.withClause, wl, out_with_clause);
    write_enum_field(buf, "override", n.r#override as i32);
}

fn out_delete_stmt(buf: &mut String, n: &::nodes::rawnodes::DeleteStmt<'_>, wl: bool) {
    buf.push_str("DELETESTMT");
    write_opt_framed(buf, "relation", &n.relation, wl, out_range_var);
    write_node_vec_field(buf, "usingClause", &n.usingClause, wl);
    write_opt_node_field(buf, "whereClause", &n.whereClause, wl);
    write_opt_framed(buf, "returningClause", &n.returningClause, wl, out_returning_clause);
    write_opt_framed(buf, "withClause", &n.withClause, wl, out_with_clause);
}

fn out_update_stmt(buf: &mut String, n: &::nodes::rawnodes::UpdateStmt<'_>, wl: bool) {
    buf.push_str("UPDATESTMT");
    write_opt_framed(buf, "relation", &n.relation, wl, out_range_var);
    write_node_vec_field(buf, "targetList", &n.targetList, wl);
    write_opt_node_field(buf, "whereClause", &n.whereClause, wl);
    write_node_vec_field(buf, "fromClause", &n.fromClause, wl);
    write_opt_framed(buf, "returningClause", &n.returningClause, wl, out_returning_clause);
    write_opt_framed(buf, "withClause", &n.withClause, wl, out_with_clause);
}

fn out_merge_stmt(buf: &mut String, n: &::nodes::rawnodes::MergeStmt<'_>, wl: bool) {
    buf.push_str("MERGESTMT");
    write_opt_framed(buf, "relation", &n.relation, wl, out_range_var);
    write_opt_node_field(buf, "sourceRelation", &n.sourceRelation, wl);
    write_opt_node_field(buf, "joinCondition", &n.joinCondition, wl);
    write_node_vec_field(buf, "mergeWhenClauses", &n.mergeWhenClauses, wl);
    write_opt_framed(buf, "returningClause", &n.returningClause, wl, out_returning_clause);
    write_opt_framed(buf, "withClause", &n.withClause, wl, out_with_clause);
}

// ===========================================================================
// _outSelectStmt
// ===========================================================================

fn out_select_stmt(buf: &mut String, n: &::nodes::rawnodes::SelectStmt<'_>, wl: bool) {
    buf.push_str("SELECTSTMT");
    write_node_vec_field(buf, "distinctClause", &n.distinctClause, wl);
    write_opt_node_field(buf, "intoClause", &n.intoClause, wl);
    write_node_vec_field(buf, "targetList", &n.targetList, wl);
    write_node_vec_field(buf, "fromClause", &n.fromClause, wl);
    write_opt_node_field(buf, "whereClause", &n.whereClause, wl);
    write_node_vec_field(buf, "groupClause", &n.groupClause, wl);
    write_bool_field(buf, "groupDistinct", n.groupDistinct);
    write_opt_node_field(buf, "havingClause", &n.havingClause, wl);
    write_node_vec_field(buf, "windowClause", &n.windowClause, wl);
    write_node_vec_field(buf, "valuesLists", &n.valuesLists, wl);
    write_node_vec_field(buf, "sortClause", &n.sortClause, wl);
    write_opt_node_field(buf, "limitOffset", &n.limitOffset, wl);
    write_opt_node_field(buf, "limitCount", &n.limitCount, wl);
    write_enum_field(buf, "limitOption", n.limitOption as i32);
    write_node_vec_field(buf, "lockingClause", &n.lockingClause, wl);
    write_opt_framed(buf, "withClause", &n.withClause, wl, out_with_clause);
    write_enum_field(buf, "op", n.op as i32);
    write_bool_field(buf, "all", n.all);
    write_opt_framed(buf, "larg", &n.larg, wl, out_select_stmt);
    write_opt_framed(buf, "rarg", &n.rarg, wl, out_select_stmt);
}

// ===========================================================================
// _outA_Const — custom.
// ===========================================================================

fn out_a_const(buf: &mut String, n: &::nodes::rawnodes::A_Const<'_>, wl: bool) {
    buf.push_str("A_CONST");
    if n.isnull {
        buf.push_str(" NULL");
    } else {
        buf.push_str(" :val ");
        match &n.val {
            // The value node (Integer/Float/Boolean/String/BitString); emitted
            // as a bare token by out_node_inner.
            Some(v) => out_node_inner(buf, &**v, wl),
            None => buf.push_str("<>"),
        }
    }
    write_location_field(buf, "location", n.location, wl);
}

// ===========================================================================
// Dispatch.
// ===========================================================================

/// Dispatch the out_parse_family `Node` arms this module owns.
pub(crate) fn try_out(buf: &mut String, node: &Node<'_>, wl: bool) -> bool {
    match node.node_tag() {
        ntag::T_Query => { let n = node.expect_query(); framed(buf, |b| out_query(b, n, wl)) },
        ntag::T_RangeTblEntry => { let n = node.expect_rangetblentry(); framed(buf, |b| out_range_tbl_entry(b, n, wl)) },
        ntag::T_RTEPermissionInfo => { let n = node.expect_rtepermissioninfo(); framed(buf, |b| out_rte_perm_info(b, n, wl)) },
        ntag::T_RangeTblFunction => { let n = node.expect_rangetblfunction(); framed(buf, |b| out_range_tbl_function(b, n, wl)) },
        ntag::T_TableSampleClause => { let n = node.expect_tablesampleclause(); framed(buf, |b| out_table_sample_clause(b, n, wl)) },
        ntag::T_SortGroupClause => { let n = node.expect_sortgroupclause(); framed(buf, |b| out_sort_group_clause(b, n, wl)) },
        ntag::T_GroupingSet => { let n = node.expect_groupingset(); framed(buf, |b| out_grouping_set(b, n, wl)) },
        ntag::T_WindowClause => { let n = node.expect_windowclause(); framed(buf, |b| out_window_clause(b, n, wl)) },
        ntag::T_RowMarkClause => { let n = node.expect_rowmarkclause(); framed(buf, |b| out_row_mark_clause(b, n, wl)) },
        ntag::T_WithCheckOption => { let n = node.expect_withcheckoption(); framed(buf, |b| out_with_check_option(b, n, wl)) },
        ntag::T_CTECycleClause => { let n = node.expect_ctecycleclause(); framed(buf, |b| out_cte_cycle_clause(b, n, wl)) },
        ntag::T_SetOperationStmt => { let n = node.expect_setoperationstmt(); framed(buf, |b| out_set_operation_stmt(b, n, wl)) },
        ntag::T_Alias => { let n = node.expect_alias(); framed(buf, |b| out_alias(b, n, wl)) },
        ntag::T_RangeVar => { let n = node.expect_rangevar(); framed(buf, |b| out_range_var(b, n, wl)) },
        ntag::T_TypeName => { let n = node.expect_typename(); framed(buf, |b| out_type_name(b, n, wl)) },
        ntag::T_ColumnDef => { let n = node.expect_columndef(); framed(buf, |b| out_column_def(b, n, wl)) },
        ntag::T_RangeTblRef => { let n = node.expect_rangetblref(); framed(buf, |b| out_range_tbl_ref(b, n, wl)) },
        ntag::T_JoinExpr => { let n = node.expect_joinexpr(); framed(buf, |b| out_join_expr(b, n, wl)) },
        ntag::T_FromExpr => { let n = node.expect_fromexpr(); framed(buf, |b| out_from_expr(b, n, wl)) },
        ntag::T_OnConflictExpr => { let n = node.expect_onconflictexpr(); framed(buf, |b| out_on_conflict_expr(b, n, wl)) },
        ntag::T_MergeAction => { let n = node.expect_mergeaction(); framed(buf, |b| out_merge_action(b, n, wl)) },
        ntag::T_LockingClause => { let n = node.expect_lockingclause(); framed(buf, |b| out_locking_clause(b, n, wl)) },
        ntag::T_ColumnRef => { let n = node.expect_columnref(); framed(buf, |b| out_column_ref(b, n, wl)) },
        ntag::T_ParamRef => { let n = node.expect_paramref(); framed(buf, |b| out_param_ref(b, n, wl)) },
        ntag::T_A_Expr => { let n = node.expect_a_expr(); framed(buf, |b| out_a_expr(b, n, wl)) },
        ntag::T_FuncCall => { let n = node.expect_funccall(); framed(buf, |b| out_func_call(b, n, wl)) },
        ntag::T_A_Star => { let n = node.expect_a_star(); framed(buf, |b| out_a_star(b, n, wl)) },
        ntag::T_A_Indices => { let n = node.expect_a_indices(); framed(buf, |b| out_a_indices(b, n, wl)) },
        ntag::T_A_Indirection => { let n = node.expect_a_indirection(); framed(buf, |b| out_a_indirection(b, n, wl)) },
        ntag::T_A_ArrayExpr => { let n = node.expect_a_arrayexpr(); framed(buf, |b| out_a_array_expr(b, n, wl)) },
        ntag::T_ResTarget => { let n = node.expect_restarget(); framed(buf, |b| out_res_target(b, n, wl)) },
        ntag::T_MultiAssignRef => { let n = node.expect_multiassignref(); framed(buf, |b| out_multi_assign_ref(b, n, wl)) },
        ntag::T_TypeCast => { let n = node.expect_typecast(); framed(buf, |b| out_type_cast(b, n, wl)) },
        ntag::T_CollateClause => { let n = node.expect_collateclause(); framed(buf, |b| out_collate_clause(b, n, wl)) },
        ntag::T_SortBy => { let n = node.expect_sortby(); framed(buf, |b| out_sort_by(b, n, wl)) },
        ntag::T_WindowDef => { let n = node.expect_windowdef(); framed(buf, |b| out_window_def(b, n, wl)) },
        ntag::T_RangeSubselect => { let n = node.expect_rangesubselect(); framed(buf, |b| out_range_subselect(b, n, wl)) },
        ntag::T_RangeFunction => { let n = node.expect_rangefunction(); framed(buf, |b| out_range_function(b, n, wl)) },
        ntag::T_RangeTableSample => { let n = node.expect_rangetablesample(); framed(buf, |b| out_range_table_sample(b, n, wl)) },
        ntag::T_WithClause => { let n = node.expect_withclause(); framed(buf, |b| out_with_clause(b, n, wl)) },
        ntag::T_InferClause => { let n = node.expect_inferclause(); framed(buf, |b| out_infer_clause(b, n, wl)) },
        ntag::T_OnConflictClause => { let n = node.expect_onconflictclause(); framed(buf, |b| out_on_conflict_clause(b, n, wl)) },
        ntag::T_MergeWhenClause => { let n = node.expect_mergewhenclause(); framed(buf, |b| out_merge_when_clause(b, n, wl)) },
        ntag::T_ReturningClause => { let n = node.expect_returningclause(); framed(buf, |b| out_returning_clause(b, n, wl)) },
        ntag::T_InsertStmt => { let n = node.expect_insertstmt(); framed(buf, |b| out_insert_stmt(b, n, wl)) },
        ntag::T_DeleteStmt => { let n = node.expect_deletestmt(); framed(buf, |b| out_delete_stmt(b, n, wl)) },
        ntag::T_UpdateStmt => { let n = node.expect_updatestmt(); framed(buf, |b| out_update_stmt(b, n, wl)) },
        ntag::T_MergeStmt => { let n = node.expect_mergestmt(); framed(buf, |b| out_merge_stmt(b, n, wl)) },
        ntag::T_SelectStmt => { let n = node.expect_selectstmt(); framed(buf, |b| out_select_stmt(b, n, wl)) },
        ntag::T_A_Const => { let n = node.expect_a_const(); framed(buf, |b| out_a_const(b, n, wl)) },

        ntag::T_TableFunc => { let n = node.expect_tablefunc(); framed(buf, |b| out_table_func(b, n, wl)) },
        ntag::T_JsonTablePathScan => { let n = node.expect_jsontablepathscan(); framed(buf, |b| out_json_table_path_scan(b, n, wl)) },
        ntag::T_JsonTableSiblingJoin => { let n = node.expect_jsontablesiblingjoin(); framed(buf, |b| out_json_table_sibling_join(b, n, wl)) },

        ntag::T_CommonTableExpr => { let n = node.expect_commontableexpr(); framed(buf, |b| out_common_table_expr(b, n, wl)) },

        _ => return false,
    }
    true
}
