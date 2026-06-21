//! `utils/adt/ruleutils.c` — **F2/F3: the query / catalog deparse drivers.**
//!
//! This family builds on F0 (the deparse name-resolution engine in
//! [`crate`]) and F1 (the expression deparser in [`crate::expr_deparse`]) to
//! reconstruct full SQL statements from `Query` trees:
//!
//! * [`get_query_def`] — the entry point. Acquires rewrite locks, flattens
//!   GROUP-RTE expressions, builds a [`DeparseContext`]/[`DeparseNamespace`],
//!   and dispatches by `commandType`.
//! * SELECT: [`get_select_query_def`] / [`get_basic_select_query`] /
//!   [`get_setop_query`] / [`get_target_list`] / [`get_values_def`] /
//!   [`get_with_clause`].
//! * INSERT / UPDATE / DELETE / MERGE / UTILITY drivers + the shared
//!   `get_update_query_targetlist_def` and `get_returning_clause`.
//! * The clause helpers: ORDER BY ([`get_rule_orderby`]), GROUP BY
//!   ([`get_rule_sortgroupclause`] / [`get_rule_groupingset`]), WINDOW
//!   ([`get_rule_windowclause`] / [`get_rule_windowspec`] /
//!   [`get_window_frame_options`]), and the FROM clause
//!   ([`get_from_clause`] / [`get_from_clause_item`] / [`get_rte_alias`] /
//!   [`get_column_alias_list`] / [`get_from_clause_coldeflist`] /
//!   [`get_tablesample_def`]).
//! * The pretty-print machinery [`append_context_keyword`] /
//!   [`remove_string_info_spaces`].
//!
//! # Cross-subsystem boundaries (seam-and-panic where the owner is unported)
//!
//! * **`get_tablefunc`** (XMLTABLE / JSON_TABLE) — the XML/JSON deparsers are a
//!   later family; an `RTE_TABLEFUNC` FROM item panics precisely.
//! * **`processIndirection`** (FieldStore / assignment SubscriptingRef
//!   decoration in INSERT/UPDATE/MERGE target columns) reaches
//!   `get_typ_typrelid` / `get_attname` plus the F1 `printSubscripts` assignment
//!   path; the plain (no-indirection) target column — the overwhelmingly common
//!   case — is rendered in full, and an actual FieldStore/assignment-SubsRef
//!   panics precisely.
//! * The catalog name generators (`generate_relation_name`, `get_attname`,
//!   `get_constraint_name`, `generate_function_name`, `generate_collation_name`,
//!   `get_typcollation`, `lookup_type_cache_lt_gt_opr`), `AcquireRewriteLocks`,
//!   and `flatten_group_exprs` cross owner seams (relcache / lsyscache /
//!   typcache / rewriteHandler / optimizer-var, all unported as whole units).
//!
//! C source: `src/backend/utils/adt/ruleutils.c` (5624-7584, 6239-6932,
//! 12251-12849).

use alloc::format;
use alloc::string::String;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsenodes::{
    RangeTblEntry, RTE_CTE, RTE_FUNCTION, RTE_RELATION, RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};
use types_nodes::primnodes::Expr;
use types_nodes::rawnodes::{
    CTEMaterialize, GroupingSet, GroupingSetKind, RangeTblFunction,
    SetOperationStmt, SetOperation, SortGroupClause, WindowClause,
};
use types_nodes::rawnodes::LockClauseStrength;

use crate::expr_deparse::{
    ch_pub as ch_, get_const_expr, get_rule_expr, get_rule_expr_funccall, get_rule_expr_toplevel,
    get_rule_list_toplevel, get_variable, str_pub as str_,
};
use crate::{deparse_columns_fetch, DeparseColumns, DeparseContext, DeparseNamespace};

/* -------------------------------------------------------------------------- *
 * Constants (PostgreSQL headers, exact values).
 * -------------------------------------------------------------------------- */

/// `PRETTYINDENT_STD` (ruleutils.c 80).
const PRETTYINDENT_STD: i32 = 8;
/// `PRETTYINDENT_JOIN` (ruleutils.c 81).
const PRETTYINDENT_JOIN: i32 = 4;
/// `PRETTYINDENT_VAR` (ruleutils.c 82).
const PRETTYINDENT_VAR: i32 = 4;
/// `PRETTYINDENT_LIMIT` (ruleutils.c 84) — wrap limit.
const PRETTYINDENT_LIMIT: i32 = 40;
/// `PRETTYFLAG_PAREN` (ruleutils.c 88).
const PRETTYFLAG_PAREN: i32 = 0x0001;
/// `PRETTYFLAG_INDENT` (ruleutils.c 89).
const PRETTYFLAG_INDENT: i32 = 0x0002;

/// `LimitOption` LIMIT_OPTION_WITH_TIES.
use types_nodes::nodelimit::LimitOption;

/// `BOOLOID` (`catalog/pg_type_d.h`).
const BOOLOID: u32 = 16;

/// `INTERNALOID` (`catalog/pg_type_d.h`).
const INTERNALOID: u32 = 2281;

/// `F_UNNEST_ANYARRAY` (`catalog/pg_proc_d.h`) — `unnest(anyarray)` OID.
const F_UNNEST_ANYARRAY: u32 = 2331;

/// `OnConflictAction` (`nodes/primnodes.h`).
use types_nodes::nodes::OnConflictAction;
/// `OverridingKind` (`nodes/primnodes.h`).
use types_nodes::modifytable::{MergeMatchKind, OverridingKind};

/// Window-frame option bits (`nodes/parsenodes.h`).
const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
const FRAMEOPTION_RANGE: i32 = 0x00002;
const FRAMEOPTION_ROWS: i32 = 0x00004;
const FRAMEOPTION_GROUPS: i32 = 0x00008;
const FRAMEOPTION_BETWEEN: i32 = 0x00010;
const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
const FRAMEOPTION_START_CURRENT_ROW: i32 = 0x00200;
const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
const FRAMEOPTION_START_OFFSET_PRECEDING: i32 = 0x00800;
const FRAMEOPTION_END_OFFSET_PRECEDING: i32 = 0x01000;
const FRAMEOPTION_START_OFFSET_FOLLOWING: i32 = 0x02000;
const FRAMEOPTION_END_OFFSET_FOLLOWING: i32 = 0x04000;
const FRAMEOPTION_EXCLUDE_CURRENT_ROW: i32 = 0x08000;
const FRAMEOPTION_EXCLUDE_GROUP: i32 = 0x10000;
const FRAMEOPTION_EXCLUDE_TIES: i32 = 0x20000;
const FRAMEOPTION_START_OFFSET: i32 = FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING;
const FRAMEOPTION_END_OFFSET: i32 = FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING;

/* -------------------------------------------------------------------------- *
 * Small helpers.
 * -------------------------------------------------------------------------- */

fn elog_error(msg: String) -> PgError {
    PgError::error(msg)
}

fn missing_field(what: &str) -> PgError {
    elog_error(format!("ruleutils: missing required node field: {what}"))
}

/// A genuinely-unported owner reached by a query-deparse path (XML/JSON
/// tablefunc, FieldStore indirection). Mirrors C structure and panics rather
/// than restructuring around the gap (mirror-PG-and-panic).
fn deferred(what: &str) -> PgError {
    panic!("ruleutils query deparse: `{what}` is prerequisite-blocked (F2/F3 seam-and-panic)");
}

/// `PRETTY_PAREN(context)`.
#[inline]
fn pretty_paren(context: &DeparseContext<'_>) -> bool {
    (context.prettyFlags & PRETTYFLAG_PAREN) != 0
}

/// `PRETTY_INDENT(context)`.
#[inline]
fn pretty_indent(context: &DeparseContext<'_>) -> bool {
    (context.prettyFlags & PRETTYFLAG_INDENT) != 0
}

/// `only_marker(rte)` (ruleutils.c 550): `rte->inh ? "" : "ONLY "`.
#[inline]
fn only_marker(rte: &RangeTblEntry<'_>) -> &'static str {
    if rte.inh {
        ""
    } else {
        "ONLY "
    }
}

/// `appendStringInfo(buf, "%d", n)` — append a decimal integer.
fn str_int(context: &mut DeparseContext<'_>, n: i64) -> PgResult<()> {
    let mut tmp = [0u8; 24];
    let s = fmt_i64(n, &mut tmp);
    str_(context, s)
}

/// Render a signed integer into a stack buffer (no_std-friendly itoa).
fn fmt_i64(mut n: i64, buf: &mut [u8; 24]) -> &str {
    if n == 0 {
        buf[0] = b'0';
        return core::str::from_utf8(&buf[..1]).unwrap();
    }
    let neg = n < 0;
    let mut i = buf.len();
    // Use i128-safe absolute via wrapping; i64::MIN handled by unsigned.
    let mut un: u64 = if neg { (n as i128).unsigned_abs() as u64 } else { n as u64 };
    let _ = &mut n;
    while un > 0 {
        i -= 1;
        buf[i] = b'0' + (un % 10) as u8;
        un /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    core::str::from_utf8(&buf[i..]).unwrap()
}

/// `rt_fetch(index, rtable)` — borrow the 1-based RTE.
fn rt_fetch<'a, 'mcx>(
    index: i32,
    rtable: &'a [RangeTblEntry<'mcx>],
) -> PgResult<&'a RangeTblEntry<'mcx>> {
    let i = (index - 1) as usize;
    rtable.get(i).ok_or_else(|| {
        elog_error(format!(
            "rt_fetch: range-table index {index} out of range (len {})",
            rtable.len()
        ))
    })
}

/// `quote_identifier(ident)` — ruleutils-owned, ported in this crate.
fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>> {
    crate::quote_identifier(mcx, ident)
}

/// `generate_collation_name(collid)` — ruleutils-owned seam.
fn generate_collation_name<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<PgString<'mcx>> {
    backend_utils_adt_ruleutils_seams::generate_collation_name::call(mcx, collid)
}

/// `generate_operator_name(operid, arg1, arg2)` — ruleutils-owned seam.
fn generate_operator_name<'mcx>(
    mcx: Mcx<'mcx>,
    operid: Oid,
    arg1: Oid,
    arg2: Oid,
) -> PgResult<PgString<'mcx>> {
    backend_utils_adt_ruleutils_seams::generate_operator_name::call(mcx, operid, arg1, arg2)
}

/// `get_attname(relid, attnum, false)` — the column's *current* catalog name
/// (tracks RENAME). lsyscache owner seam.
fn get_attname<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: i16) -> PgResult<PgString<'mcx>> {
    match backend_utils_cache_lsyscache_seams::get_attname::call(mcx, relid, attnum, false)? {
        Some(s) => Ok(s),
        None => Err(elog_error(format!(
            "cache lookup failed for attribute {attnum} of relation {}",
            relid
        ))),
    }
}

/// `generate_relation_name(relid, namespaces)` — the CTE-name-conflict scan is
/// done in-crate (we own the namespace stack); the catalog half (relname,
/// visibility, schema, quoting) is the owner seam, parametrised by the forced-
/// qualification flag the conflict scan produced.
fn generate_relation_name<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    context: &DeparseContext<'mcx>,
) -> PgResult<PgString<'mcx>> {
    // C 13169-13189: force qualification if relname collides with any CTE name
    // visible in the namespace list. We need the live relname to compare; the
    // owner seam returns the qualified name with the force flag, so here we ask
    // it once with force=false, then re-ask force=true only if there's a CTE
    // conflict. To avoid double catalog hits we instead pass the conflict flag.
    //
    // Compute need_qual from the CTE lists. CTE names live in dpns.ctes as
    // CommonTableExpr nodes. We compare against the unqualified relname, which
    // the owner exposes via get_rel_name through the lsyscache seam.
    let relname = crate::get_rel_name_pub(mcx, relid)?;
    let need_qual = match relname.as_deref() {
        Some(rn) => cte_name_conflict(context, rn),
        None => false,
    };
    backend_utils_adt_ruleutils_seams::generate_relation_name::call(mcx, relid, need_qual)
}

/// Scan the namespace stack's CTE lists for a name matching `relname`
/// (ruleutils.c 13171-13188).
fn cte_name_conflict(context: &DeparseContext<'_>, relname: &str) -> bool {
    for dpns in context.namespaces.iter() {
        for cte in dpns.ctes.iter() {
            if let Some(c) = (**cte).as_commontableexpr() {
                if let Some(name) = c.ctename.as_deref() {
                    if name == relname {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/* -------------------------------------------------------------------------- *
 * appendContextKeyword / removeStringInfoSpaces (ruleutils.c 9081-9140).
 * -------------------------------------------------------------------------- */

/// `appendContextKeyword(context, str, indentBefore, indentAfter, indentPlus)`
/// (ruleutils.c 9081-9128).
fn append_context_keyword(
    context: &mut DeparseContext<'_>,
    s: &str,
    indent_before: i32,
    indent_after: i32,
    indent_plus: i32,
) -> PgResult<()> {
    if pretty_indent(context) {
        context.indentLevel += indent_before;

        // remove any trailing spaces, then add a newline and some spaces
        remove_string_info_spaces(context);
        ch_(context, b'\n')?;

        let indent_amount = if context.indentLevel < PRETTYINDENT_LIMIT {
            core::cmp::max(context.indentLevel, 0) + indent_plus
        } else {
            let mut a = PRETTYINDENT_LIMIT
                + (context.indentLevel - PRETTYINDENT_LIMIT) / (PRETTYINDENT_STD / 2);
            a %= PRETTYINDENT_LIMIT;
            a += indent_plus;
            a
        };
        spaces(context, indent_amount as usize)?;
        str_(context, s)?;

        context.indentLevel += indent_after;
        if context.indentLevel < 0 {
            context.indentLevel = 0;
        }
    } else {
        str_(context, s)?;
    }
    Ok(())
}

/// `appendStringInfoSpaces(buf, n)`.
fn spaces(context: &mut DeparseContext<'_>, n: usize) -> PgResult<()> {
    let mcx = context.buf.allocator();
    context.buf.data.try_reserve(n).map_err(|_| mcx.oom(n))?;
    for _ in 0..n {
        context.buf.data.push(b' ');
    }
    Ok(())
}

/// `removeStringInfoSpaces(str)` (ruleutils.c 9135-9140).
fn remove_string_info_spaces(context: &mut DeparseContext<'_>) {
    let buf = &mut context.buf.data;
    while !buf.is_empty() && *buf.last().unwrap() == b' ' {
        buf.pop();
    }
}

/* -------------------------------------------------------------------------- *
 * get_query_def — the entry point (ruleutils.c 5624-5717).
 * -------------------------------------------------------------------------- */

/// `get_query_def(query, buf, parentnamespace, resultDesc, colNamesVisible,
/// prettyFlags, wrapColumn, startIndent)` (ruleutils.c 5623-5717).
///
/// Renders into `context`'s already-built `buf`; the caller owns the buffer.
/// `result_desc` is the optional view tupdesc (SELECT only).
#[allow(clippy::too_many_arguments)]
pub fn get_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    buf: types_stringinfo::StringInfo<'mcx>,
    parent_namespace: &[DeparseNamespace<'mcx>],
    result_desc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    col_names_visible: bool,
    pretty_flags: i32,
    wrap_column: i32,
    start_indent: i32,
) -> PgResult<types_stringinfo::StringInfo<'mcx>> {
    // CHECK_FOR_INTERRUPTS / check_stack_depth are host-loop concerns.

    // We must scribble on a private copy of the query (C scribbles on the
    // passed tree: AcquireRewriteLocks fixes up JOIN RTEs, flatten_group_exprs
    // rewrites the targetlist/havingQual). The owned model has no shared
    // pointers, so clone first.
    let mut query = query.clone_in(mcx)?;

    let rtable_size = if query.hasGroupRTE {
        query.rtable.len() as i32 - 1
    } else {
        query.rtable.len() as i32
    };

    // Replace any GROUP-output Vars in the targetlist/havingQual with the
    // underlying grouping expressions (optimizer var.c owner seam).
    if query.hasGroupRTE {
        flatten_group_exprs_targetlist(mcx, &mut query)?;
    }

    // Acquire AccessShareLock on referenced relations and fix up deleted
    // columns in JOIN RTEs (rewriteHandler owner seam).
    backend_utils_adt_ruleutils_seams::acquire_rewrite_locks::call(mcx, &mut query, false, false)?;

    // Build the deparse context.
    let mut dpns = DeparseNamespace::zeroed(mcx);
    crate::set_deparse_for_query(mcx, &mut dpns, &query, parent_namespace)?;

    // context.namespaces = lcons(&dpns, list_copy(parentnamespace));
    let mut namespaces: PgVec<'mcx, DeparseNamespace<'mcx>> = PgVec::new_in(mcx);
    namespaces.try_reserve(1 + parent_namespace.len()).map_err(|_| mcx.oom(0))?;
    namespaces.push(dpns);
    for p in parent_namespace.iter() {
        namespaces.push(crate::clone_namespace_pub(mcx, p)?);
    }

    let mut context = DeparseContext {
        buf,
        namespaces,
        resultDesc: None,
        targetList: PgVec::new_in(mcx),
        windowClause: PgVec::new_in(mcx),
        prettyFlags: pretty_flags,
        wrapColumn: wrap_column,
        indentLevel: start_indent,
        varprefix: !parent_namespace.is_empty() || rtable_size != 1,
        colNamesVisible: col_names_visible,
        inGroupBy: false,
        varInOrderBy: false,
        appendparents: None,
    };

    match query.commandType {
        CmdType::CMD_SELECT => {
            context.resultDesc = result_desc;
            get_select_query_def(mcx, &query, &mut context)?;
        }
        CmdType::CMD_UPDATE => get_update_query_def(mcx, &query, &mut context)?,
        CmdType::CMD_INSERT => get_insert_query_def(mcx, &query, &mut context)?,
        CmdType::CMD_DELETE => get_delete_query_def(mcx, &query, &mut context)?,
        CmdType::CMD_MERGE => get_merge_query_def(mcx, &query, &mut context)?,
        CmdType::CMD_NOTHING => str_(&mut context, "NOTHING")?,
        CmdType::CMD_UTILITY => get_utility_query_def(mcx, &query, &mut context)?,
        other => {
            return Err(elog_error(format!(
                "unrecognized query command type: {}",
                other as i32
            )))
        }
    }

    Ok(context.buf)
}

/// `flatten_group_exprs(NULL, query, (Node *) query->targetList)` +
/// `flatten_group_exprs(NULL, query, query->havingQual)` (ruleutils.c
/// 5644-5650). Reached only when `query->hasGroupRTE` (a query that went
/// through grouping-set planning). The owner (optimizer var.c
/// `flatten_group_exprs`) is unported as a whole unit, and the targetlist is
/// passed as a `List *` node which our value-typed `PgVec<TargetEntry>` carrier
/// does not project to — so this is a seam-and-panic that names both call
/// sites. The common deparse path (no GROUP RTE) never reaches it.
fn flatten_group_exprs_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut Query<'mcx>,
) -> PgResult<()> {
    // Mirror C's havingQual rewrite shape (the targetList List* carrier is the
    // contract-divergent half); the seam owner is unported, so this panics
    // precisely rather than silently skipping the targetlist flattening.
    let snapshot = query.clone_in(mcx)?;
    let hq = query
        .havingQual
        .as_deref()
        .map(|e| -> PgResult<_> { Ok(Node::mk_expr(mcx, e.clone_in(mcx)?)?) })
        .transpose()?
        .unwrap_or(Node::mk_list(mcx, PgVec::new_in(mcx))?);
    let boxed = mcx::alloc_in(mcx, hq)?;
    let _ = backend_utils_adt_ruleutils_seams::flatten_group_exprs::call(mcx, &snapshot, &boxed)?;
    // (Unreachable: the seam owner is unported and panics. Kept to wire the
    // faithful call shape — flatten_group_exprs over the targetList List* is the
    // companion call the owner installs alongside.)
    Err(deferred(
        "get_query_def hasGroupRTE (flatten_group_exprs over targetList List*; optimizer var.c + List-carrier)",
    ))
}

/* -------------------------------------------------------------------------- *
 * get_values_def / get_with_clause (ruleutils.c 5723-5899).
 * -------------------------------------------------------------------------- */

/// `get_values_def(values_lists, context)` (ruleutils.c 5723-5760).
fn get_values_def<'mcx>(
    values_lists: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    str_(context, "VALUES ")?;

    let mut first_list = true;
    for vtl in values_lists.iter() {
        if first_list {
            first_list = false;
        } else {
            str_(context, ", ")?;
        }

        ch_(context, b'(')?;
        // each element of values_lists is a List of column expressions.
        let sublist = (**vtl).as_list().ok_or_else(|| {
            elog_error(format!(
                "VALUES sublist is not a List (tag {})",
                vtl.tag().0
            ))
        })?;
        let mut first_col = true;
        for col in sublist.iter() {
            if first_col {
                first_col = false;
            } else {
                ch_(context, b',')?;
            }
            get_rule_expr_toplevel(col, context, false)?;
        }
        ch_(context, b')')?;
    }
    Ok(())
}

/// `get_with_clause(query, context)` (ruleutils.c 5766-5899).
fn get_with_clause<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if query.cteList.is_empty() {
        return Ok(());
    }

    if pretty_indent(context) {
        context.indentLevel += PRETTYINDENT_STD;
        ch_(context, b' ')?;
    }

    let mut sep: &str = if query.hasRecursive {
        "WITH RECURSIVE "
    } else {
        "WITH "
    };

    for l in query.cteList.iter() {
        let cte = (**l).as_commontableexpr().ok_or_else(|| {
            elog_error(format!(
                "cteList element is not a CommonTableExpr (tag {})",
                l.tag().0
            ))
        })?;

        str_(context, sep)?;
        let ctename = cte.ctename.as_deref().ok_or_else(|| missing_field("CommonTableExpr.ctename"))?;
        let q = quote_identifier(mcx, ctename)?;
        str_(context, q.as_str())?;
        if !cte.aliascolnames.is_empty() {
            ch_(context, b'(')?;
            let mut first = true;
            for c in cte.aliascolnames.iter() {
                if first {
                    first = false;
                } else {
                    str_(context, ", ")?;
                }
                let q = quote_identifier(mcx, crate::str_val_pub(c)?)?;
                str_(context, q.as_str())?;
            }
            ch_(context, b')')?;
        }
        str_(context, " AS ")?;
        match cte.ctematerialized {
            CTEMaterialize::CTEMaterializeDefault => {}
            CTEMaterialize::CTEMaterializeAlways => str_(context, "MATERIALIZED ")?,
            CTEMaterialize::CTEMaterializeNever => str_(context, "NOT MATERIALIZED ")?,
        }
        ch_(context, b'(')?;
        if pretty_indent(context) {
            append_context_keyword(context, "", 0, 0, 0)?;
        }
        // recurse into the CTE's Query
        let ctequery = cte.ctequery.as_deref().ok_or_else(|| missing_field("CommonTableExpr.ctequery"))?;
        let subq = match ctequery.node_tag() {
            types_nodes::nodes::ntag::T_Query => ctequery.expect_query(),
            _ => {
                return Err(elog_error(format!(
                    "CTE query is not a Query (tag {})",
                    ctequery.tag().0
                )))
            }
        };
        recurse_query_def(mcx, subq, context, None, true)?;
        if pretty_indent(context) {
            append_context_keyword(context, "", 0, 0, 0)?;
        }
        ch_(context, b')')?;

        if let Some(sc) = cte.search_clause.as_deref() {
            str_(context, " SEARCH ")?;
            str_(context, if sc.search_breadth_first { "BREADTH" } else { "DEPTH" })?;
            str_(context, " FIRST BY ")?;
            let mut first = true;
            for lc in sc.search_col_list.iter() {
                if first {
                    first = false;
                } else {
                    str_(context, ", ")?;
                }
                let q = quote_identifier(mcx, crate::str_val_pub(lc)?)?;
                str_(context, q.as_str())?;
            }
            str_(context, " SET ")?;
            let q = quote_identifier(mcx, sc.search_seq_column.as_deref().ok_or_else(|| missing_field("search_seq_column"))?)?;
            str_(context, q.as_str())?;
        }

        if let Some(cyc_node) = cte.cycle_clause.as_deref() {
            let cc = cyc_node.as_ctecycleclause().ok_or_else(|| {
                elog_error(format!(
                    "cycle_clause is not a CTECycleClause (tag {})",
                    cyc_node.tag().0
                ))
            })?;
            str_(context, " CYCLE ")?;
            let mut first = true;
            for lc in cc.cycle_col_list.iter() {
                if first {
                    first = false;
                } else {
                    str_(context, ", ")?;
                }
                let q = quote_identifier(mcx, crate::str_val_pub(lc)?)?;
                str_(context, q.as_str())?;
            }
            str_(context, " SET ")?;
            let q = quote_identifier(mcx, cc.cycle_mark_column.as_deref().ok_or_else(|| missing_field("cycle_mark_column"))?)?;
            str_(context, q.as_str())?;

            // TO/DEFAULT, unless the default bool true/false pair.
            let cmv = cc.cycle_mark_value.as_deref().ok_or_else(|| missing_field("cycle_mark_value"))?;
            let cmd = cc.cycle_mark_default.as_deref().ok_or_else(|| missing_field("cycle_mark_default"))?;
            let is_default = is_const_bool(cmv, true) && is_const_bool(cmd, false);
            if !is_default {
                str_(context, " TO ")?;
                get_rule_expr(cmv, context, false)?;
                str_(context, " DEFAULT ")?;
                get_rule_expr(cmd, context, false)?;
            }

            str_(context, " USING ")?;
            let q = quote_identifier(mcx, cc.cycle_path_column.as_deref().ok_or_else(|| missing_field("cycle_path_column"))?)?;
            str_(context, q.as_str())?;
        }

        sep = ", ";
    }

    if pretty_indent(context) {
        context.indentLevel -= PRETTYINDENT_STD;
        append_context_keyword(context, "", 0, 0, 0)?;
    } else {
        ch_(context, b' ')?;
    }
    Ok(())
}

/// Test whether a node is a non-null boolean Const equal to `want`
/// (ruleutils.c 5876-5877).
fn is_const_bool(node: &Node<'_>, want: bool) -> bool {
    if let Some(c) = node.as_const() {
        if c.consttype == BOOLOID && !c.constisnull {
            // DatumGetBool: the Datum's low byte.
            return c.constvalue.as_bool() == want;
        }
    }
    false
}

/* -------------------------------------------------------------------------- *
 * get_select_query_def + get_basic_select_query (ruleutils.c 5905-6230).
 * -------------------------------------------------------------------------- */

/// `get_select_query_def(query, context)` (ruleutils.c 5905-6031).
fn get_select_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(mcx, query, context)?;

    // Subroutines may need to consult the SELECT targetlist and windowClause.
    context.targetList = clone_targetlist(mcx, &query.targetList)?;
    context.windowClause = clone_window_clauses(mcx, &query.windowClause)?;

    let force_colno;
    if query.setOperations.is_some() {
        let setop = query.setOperations.as_deref().unwrap();
        get_setop_query(mcx, setop, query, context)?;
        force_colno = true;
    } else {
        get_basic_select_query(mcx, query, context)?;
        force_colno = false;
    }

    // ORDER BY
    if !query.sortClause.is_empty() {
        append_context_keyword(context, " ORDER BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        let order = node_vec_to_sgc(mcx, &query.sortClause)?;
        get_rule_orderby(mcx, &order, &query.targetList, force_colno, context)?;
    }

    // LIMIT / OFFSET
    if let Some(off) = query.limitOffset.as_deref() {
        append_context_keyword(context, " OFFSET ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?;
        get_rule_expr(&Node::mk_expr(mcx, off.clone_in(mcx)?)?, context, false)?;
    }
    if let Some(cnt) = query.limitCount.as_deref() {
        if query.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
            append_context_keyword(context, " FETCH FIRST ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?;
            ch_(context, b'(')?;
            get_rule_expr(&Node::mk_expr(mcx, cnt.clone_in(mcx)?)?, context, false)?;
            ch_(context, b')')?;
            str_(context, " ROWS WITH TIES")?;
        } else {
            append_context_keyword(context, " LIMIT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?;
            if let Expr::Const(c) = cnt {
                if c.constisnull {
                    str_(context, "ALL")?;
                } else {
                    get_rule_expr(&Node::mk_expr(mcx, cnt.clone_in(mcx)?)?, context, false)?;
                }
            } else {
                get_rule_expr(&Node::mk_expr(mcx, cnt.clone_in(mcx)?)?, context, false)?;
            }
        }
    }

    // FOR [KEY] UPDATE/SHARE
    if query.hasForUpdate {
        for l in query.rowMarks.iter() {
            let rc = *(**l).as_rowmarkclause().ok_or_else(|| {
                elog_error(format!(
                    "rowMarks element is not a RowMarkClause (tag {})",
                    l.tag().0
                ))
            })?;
            if rc.pushedDown {
                continue;
            }
            match rc.strength {
                LockClauseStrength::LCS_NONE => {
                    return Err(elog_error("unrecognized LockClauseStrength 0".into()))
                }
                LockClauseStrength::LCS_FORKEYSHARE => {
                    append_context_keyword(context, " FOR KEY SHARE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?
                }
                LockClauseStrength::LCS_FORSHARE => {
                    append_context_keyword(context, " FOR SHARE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?
                }
                LockClauseStrength::LCS_FORNOKEYUPDATE => {
                    append_context_keyword(context, " FOR NO KEY UPDATE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?
                }
                LockClauseStrength::LCS_FORUPDATE => {
                    append_context_keyword(context, " FOR UPDATE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?
                }
            }
            str_(context, " OF ")?;
            let name = crate::get_rtable_name_pub(rc.rti as i32, context)?;
            let q = quote_identifier(mcx, name.unwrap_or(""))?;
            str_(context, q.as_str())?;
            if rc.waitPolicy == types_nodes::rawnodes::LockWaitError {
                str_(context, " NOWAIT")?;
            } else if rc.waitPolicy == types_nodes::rawnodes::LockWaitSkip {
                str_(context, " SKIP LOCKED")?;
            }
        }
    }
    Ok(())
}

/// `get_simple_values_rte(query, resultDesc)` (ruleutils.c 6038-6105). Returns
/// the index (1-based) of a simple VALUES RTE, or None.
fn get_simple_values_rte<'mcx>(
    query: &Query<'mcx>,
    result_desc: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
) -> PgResult<Option<usize>> {
    let mut result: Option<usize> = None;
    for (i, rte) in query.rtable.iter().enumerate() {
        if rte.rtekind == RTE_VALUES && rte.inFromCl {
            if result.is_some() {
                return Ok(None); // multiple VALUES
            }
            result = Some(i);
        } else if rte.rtekind == RTE_RELATION && !rte.inFromCl {
            continue; // ignore rule entries
        } else {
            return Ok(None); // something else
        }
    }

    if let Some(ri) = result {
        let rte = &query.rtable[ri];
        let eref = rte.eref.as_ref().ok_or_else(|| missing_field("VALUES RTE eref"))?;
        if query.targetList.len() != eref.colnames.len() {
            return Ok(None);
        }
        let mut colno = 0usize;
        for (tle, cname_node) in query.targetList.iter().zip(eref.colnames.iter()) {
            if tle.resjunk {
                return Ok(None);
            }
            let cname = crate::str_val_pub(cname_node)?;
            colno += 1;
            // compute name get_target_list would use
            let colname: Option<String> = match result_desc {
                Some(rd) if colno <= rd.natts as usize => {
                    Some(tupdesc_attname(rd, colno - 1)?)
                }
                _ => tle.resname.as_deref().map(String::from),
            };
            match colname {
                Some(c) if c == cname => {}
                _ => return Ok(None),
            }
        }
    }
    Ok(result)
}

/// `get_basic_select_query(query, context)` (ruleutils.c 6107-6230).
fn get_basic_select_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if pretty_indent(context) {
        context.indentLevel += PRETTYINDENT_STD;
        ch_(context, b' ')?;
    }

    // SELECT * FROM (VALUES ...) collapse.
    let rd = context.resultDesc.as_deref().map(|d| d as *const _);
    let values_rte = {
        let rd_ref = context.resultDesc.as_deref();
        get_simple_values_rte(query, rd_ref)?
    };
    let _ = rd;
    if let Some(ri) = values_rte {
        get_values_def(&query.rtable[ri].values_lists, context)?;
        return Ok(());
    }

    if query.isReturn {
        str_(context, "RETURN")?;
    } else {
        str_(context, "SELECT")?;
    }

    // DISTINCT
    if !query.distinctClause.is_empty() {
        if query.hasDistinctOn {
            str_(context, " DISTINCT ON (")?;
            let mut sep = "";
            for l in query.distinctClause.iter() {
                let srt = as_sortgroupclause(l)?;
                str_(context, sep)?;
                get_rule_sortgroupclause(mcx, srt.tleSortGroupRef, &query.targetList, false, context)?;
                sep = ", ";
            }
            ch_(context, b')')?;
        } else {
            str_(context, " DISTINCT")?;
        }
    }

    // target list
    get_target_list(mcx, &query.targetList, context)?;

    // FROM
    get_from_clause(mcx, query, " FROM ", context)?;

    // WHERE
    let jointree = query.jointree.as_deref().ok_or_else(|| missing_field("SELECT jointree"))?;
    if let Some(quals) = jointree.quals.as_deref() {
        append_context_keyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        get_rule_expr(quals, context, false)?;
    }

    // GROUP BY
    if !query.groupClause.is_empty() || !query.groupingSets.is_empty() {
        append_context_keyword(context, " GROUP BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        if query.groupDistinct {
            str_(context, "DISTINCT ")?;
        }
        let save_ingroupby = context.inGroupBy;
        context.inGroupBy = true;

        if query.groupingSets.is_empty() {
            let mut sep = "";
            for l in query.groupClause.iter() {
                let grp = as_sortgroupclause(l)?;
                str_(context, sep)?;
                get_rule_sortgroupclause(mcx, grp.tleSortGroupRef, &query.targetList, false, context)?;
                sep = ", ";
            }
        } else {
            let mut sep = "";
            for l in query.groupingSets.iter() {
                let grp = as_groupingset(l)?;
                str_(context, sep)?;
                get_rule_groupingset(mcx, grp, &query.targetList, true, context)?;
                sep = ", ";
            }
        }
        context.inGroupBy = save_ingroupby;
    }

    // HAVING
    if let Some(hq) = query.havingQual.as_deref() {
        append_context_keyword(context, " HAVING ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0)?;
        get_rule_expr(&Node::mk_expr(mcx, hq.clone_in(mcx)?)?, context, false)?;
    }

    // WINDOW
    if !query.windowClause.is_empty() {
        get_rule_windowclause(mcx, query, context)?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_target_list / get_returning_clause (ruleutils.c 6238-6411).
 * -------------------------------------------------------------------------- */

/// `get_target_list(targetList, context)` (ruleutils.c 6238-6372).
///
/// The pretty-print line-wrapping path (a separate targetbuf with newline
/// inspection) is followed faithfully; the common non-pretty path
/// (`wrapColumn < 0` / no INDENT) just appends each field.
fn get_target_list<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut last_was_multiline = false;
    let mut sep = " ";
    let mut colno = 0i32;

    for tle in target_list.iter() {
        if tle.resjunk {
            continue;
        }
        str_(context, sep)?;
        sep = ", ";
        colno += 1;

        // Render the TLE text into a temporary buffer (targetbuf).
        let saved = core::mem::replace(&mut context.buf, types_stringinfo::StringInfo::new_in(mcx));

        let attname: Option<PgString<'mcx>> = match tle.expr.as_deref() {
            Some(Expr::Var(var)) => get_variable(var, 0, true, context)?,
            Some(e) => {
                get_rule_expr(&Node::mk_expr(mcx, e.clone_in(mcx)?)?, context, true)?;
                if context.colNamesVisible {
                    None
                } else {
                    Some(PgString::from_str_in("?column?", mcx)?)
                }
            }
            None => None,
        };

        // Figure out the result column name.
        let colname: Option<PgString<'mcx>> = match context.resultDesc.as_deref() {
            Some(rd) if colno <= rd.natts => {
                Some(PgString::from_str_in(&tupdesc_attname(rd, (colno - 1) as usize)?, mcx)?)
            }
            _ => match tle.resname.as_deref() {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            },
        };

        // Show AS unless the column name is already correct.
        if let Some(cn) = colname.as_deref() {
            let needs_as = match attname.as_deref() {
                None => true,
                Some(an) => an != cn,
            };
            if needs_as {
                str_(context, " AS ")?;
                let q = quote_identifier(mcx, cn)?;
                str_(context, q.as_str())?;
            }
        }

        // Restore output buffer; `targetbuf` is now what we just built.
        let targetbuf = core::mem::replace(&mut context.buf, saved);

        // Pretty line-wrapping.
        if pretty_indent(context) && context.wrapColumn >= 0 {
            let leading_nl = !targetbuf.data.is_empty() && targetbuf.data[0] == b'\n';
            if leading_nl {
                remove_string_info_spaces(context);
            } else {
                let trailing_len = trailing_line_len(&context.buf.data);
                if colno > 1
                    && ((trailing_len + targetbuf.data.len() > context.wrapColumn as usize)
                        || last_was_multiline)
                {
                    append_context_keyword(context, "", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_VAR)?;
                }
            }
            // multiline status for next iteration
            let start = if leading_nl { 1 } else { 0 };
            last_was_multiline = targetbuf.data[start..].contains(&b'\n');
        }

        // Append the field.
        append_binary(context, &targetbuf.data)?;
    }
    Ok(())
}

/// `get_returning_clause(query, context)` (ruleutils.c 6374-6411).
fn get_returning_clause<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if query.returningList.is_empty() {
        return Ok(());
    }
    append_context_keyword(context, " RETURNING", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;

    let mut have_with = false;
    if let Some(old) = query.returningOldAlias.as_deref() {
        if old != "old" {
            str_(context, " WITH (OLD AS ")?;
            let q = quote_identifier(mcx, old)?;
            str_(context, q.as_str())?;
            have_with = true;
        }
    }
    if let Some(new) = query.returningNewAlias.as_deref() {
        if new != "new" {
            if have_with {
                str_(context, ", NEW AS ")?;
                let q = quote_identifier(mcx, new)?;
                str_(context, q.as_str())?;
            } else {
                str_(context, " WITH (NEW AS ")?;
                let q = quote_identifier(mcx, new)?;
                str_(context, q.as_str())?;
                have_with = true;
            }
        }
    }
    if have_with {
        ch_(context, b')')?;
    }

    get_target_list(mcx, &query.returningList, context)
}

/* -------------------------------------------------------------------------- *
 * get_setop_query (ruleutils.c 6413-6554).
 * -------------------------------------------------------------------------- */

/// `get_setop_query(setOp, query, context)` (ruleutils.c 6413-6554).
fn get_setop_query<'mcx>(
    mcx: Mcx<'mcx>,
    set_op: &Node<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match set_op.node_tag() {
        types_nodes::nodes::ntag::T_RangeTblRef => {
            let rtr = set_op.expect_rangetblref();
            let rte = rt_fetch(rtr.rtindex, &query.rtable)?;
            let subquery = rte.subquery.as_deref().ok_or_else(|| missing_field("setop leaf subquery"))?;
            let need_paren = !subquery.cteList.is_empty()
                || !subquery.sortClause.is_empty()
                || !subquery.rowMarks.is_empty()
                || subquery.limitOffset.is_some()
                || subquery.limitCount.is_some()
                || subquery.setOperations.is_some();
            if need_paren {
                ch_(context, b'(')?;
            }
            let rd = clone_opt_tupdesc(mcx, context.resultDesc.as_deref())?;
            recurse_query_def(mcx, subquery, context, rd, context.colNamesVisible)?;
            if need_paren {
                ch_(context, b')')?;
            }
            Ok(())
        }
        types_nodes::nodes::ntag::T_SetOperationStmt => {
            get_setop_stmt(mcx, set_op.expect_setoperationstmt(), query, context)
        }
        _ => Err(elog_error(format!("unrecognized node type: {}", set_op.tag().0))),
    }
}

fn get_setop_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    op: &SetOperationStmt<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let larg = op.larg.as_deref().ok_or_else(|| missing_field("SetOperationStmt.larg"))?;
    let rarg = op.rarg.as_deref().ok_or_else(|| missing_field("SetOperationStmt.rarg"))?;

    // LHS parens.
    let mut need_paren = if let Some(lop) = larg.as_setoperationstmt() {
        !(op.op == lop.op && op.all == lop.all)
    } else {
        false
    };

    let mut subindent;
    if need_paren {
        ch_(context, b'(')?;
        subindent = PRETTYINDENT_STD;
        append_context_keyword(context, "", subindent, 0, 0)?;
    } else {
        subindent = 0;
    }

    get_setop_query(mcx, larg, query, context)?;

    if need_paren {
        append_context_keyword(context, ") ", -subindent, 0, 0)?;
    } else if pretty_indent(context) {
        append_context_keyword(context, "", -subindent, 0, 0)?;
    } else {
        ch_(context, b' ')?;
    }

    match op.op {
        SetOperation::SETOP_UNION => str_(context, "UNION ")?,
        SetOperation::SETOP_INTERSECT => str_(context, "INTERSECT ")?,
        SetOperation::SETOP_EXCEPT => str_(context, "EXCEPT ")?,
        other => return Err(elog_error(format!("unrecognized set op: {}", other as i32))),
    }
    if op.all {
        str_(context, "ALL ")?;
    }

    // RHS parens.
    need_paren = rarg.is_setoperationstmt();
    if need_paren {
        ch_(context, b'(')?;
        subindent = PRETTYINDENT_STD;
    } else {
        subindent = 0;
    }
    append_context_keyword(context, "", subindent, 0, 0)?;

    let save_colnamesvisible = context.colNamesVisible;
    context.colNamesVisible = false;
    get_setop_query(mcx, rarg, query, context)?;
    context.colNamesVisible = save_colnamesvisible;

    if pretty_indent(context) {
        context.indentLevel -= subindent;
    }
    if need_paren {
        append_context_keyword(context, ")", 0, 0, 0)?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * get_rule_sortgroupclause / groupingset / orderby / window
 * (ruleutils.c 6561-6900).
 * -------------------------------------------------------------------------- */

/// `get_rule_sortgroupclause(ref, tlist, force_colno, context)` (ruleutils.c
/// 6561-6625). Returns a clone of the referenced expression (for the caller's
/// type lookup, as C returns the Node*).
fn get_rule_sortgroupclause<'mcx>(
    mcx: Mcx<'mcx>,
    sortref: u32,
    tlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    force_colno: bool,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<Option<Expr>> {
    let tle = get_sortgroupref_tle(sortref, tlist)?;
    let expr_opt: Option<Expr> = match tle.expr.as_deref() {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };

    if force_colno {
        debug_assert!(!tle.resjunk);
        str_int(context, tle.resno as i64)?;
    } else if let Some(expr) = expr_opt.as_ref() {
        match expr {
            Expr::Const(_) => {
                get_const_expr(&Node::mk_expr(mcx, expr.clone_in(mcx)?)?, context, 1)?;
            }
            Expr::Var(var) => {
                let save = context.varInOrderBy;
                context.varInOrderBy = true;
                let _ = get_variable(var, 0, false, context)?;
                context.varInOrderBy = save;
            }
            _ => {
                let need_paren = pretty_paren(context)
                    || matches!(
                        expr,
                        Expr::FuncExpr(_) | Expr::Aggref(_) | Expr::WindowFunc(_) | Expr::JsonConstructorExpr(_)
                    );
                if need_paren {
                    ch_(context, b'(')?;
                }
                get_rule_expr(&Node::mk_expr(mcx, expr.clone_in(mcx)?)?, context, true)?;
                if need_paren {
                    ch_(context, b')')?;
                }
            }
        }
    }
    // else: do nothing (expr is None, probably can't happen)

    Ok(expr_opt)
}

/// `get_rule_groupingset(gset, targetlist, omit_parens, context)` (ruleutils.c
/// 6630-6685).
fn get_rule_groupingset<'mcx>(
    mcx: Mcx<'mcx>,
    gset: &GroupingSet<'mcx>,
    targetlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    omit_parens: bool,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut omit_child_parens = true;
    let mut sep = "";

    match gset.kind {
        GroupingSetKind::GROUPING_SET_EMPTY => {
            str_(context, "()")?;
            return Ok(());
        }
        GroupingSetKind::GROUPING_SET_SIMPLE => {
            let wrap = !omit_parens || gset.content.len() != 1;
            if wrap {
                ch_(context, b'(')?;
            }
            for l in gset.content.iter() {
                let r = node_as_int(l)?;
                str_(context, sep)?;
                get_rule_sortgroupclause(mcx, r as u32, targetlist, false, context)?;
                sep = ", ";
            }
            if wrap {
                ch_(context, b')')?;
            }
            return Ok(());
        }
        GroupingSetKind::GROUPING_SET_ROLLUP => str_(context, "ROLLUP(")?,
        GroupingSetKind::GROUPING_SET_CUBE => str_(context, "CUBE(")?,
        GroupingSetKind::GROUPING_SET_SETS => {
            str_(context, "GROUPING SETS (")?;
            omit_child_parens = false;
        }
    }

    for l in gset.content.iter() {
        let child = as_groupingset(l)?;
        str_(context, sep)?;
        get_rule_groupingset(mcx, child, targetlist, omit_child_parens, context)?;
        sep = ", ";
    }
    ch_(context, b')')?;
    Ok(())
}

/// `get_rule_orderby(orderList, targetList, force_colno, context)` (ruleutils.c
/// 6690-6740). **Public**: F1's aggregate ORDER BY / WITHIN GROUP reaches it.
pub fn get_rule_orderby<'mcx>(
    mcx: Mcx<'mcx>,
    order_list: &[SortGroupClause],
    target_list: &[types_nodes::primnodes::TargetEntry<'mcx>],
    force_colno: bool,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut sep = "";
    for srt in order_list.iter() {
        str_(context, sep)?;
        let sortexpr = get_rule_sortgroupclause(mcx, srt.tleSortGroupRef, target_list, force_colno, context)?;
        let sortcoltype = match sortexpr.as_ref() {
            Some(e) => backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?.typid,
            None => Oid::default(),
        };
        let (lt_opr, gt_opr) =
            backend_utils_adt_ruleutils_seams::lookup_type_cache_lt_gt_opr::call(sortcoltype)?;
        if srt.sortop == lt_opr {
            // ASC is default
            if srt.nulls_first {
                str_(context, " NULLS FIRST")?;
            }
        } else if srt.sortop == gt_opr {
            str_(context, " DESC")?;
            if !srt.nulls_first {
                str_(context, " NULLS LAST")?;
            }
        } else {
            str_(context, " USING ")?;
            let opn = generate_operator_name(mcx, srt.sortop, sortcoltype, sortcoltype)?;
            str_(context, opn.as_str())?;
            if srt.nulls_first {
                str_(context, " NULLS FIRST")?;
            } else {
                str_(context, " NULLS LAST")?;
            }
        }
        sep = ", ";
    }
    Ok(())
}

/// `get_rule_windowclause(query, context)` (ruleutils.c 6748-6775).
fn get_rule_windowclause<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut sep: Option<&str> = None;
    for l in query.windowClause.iter() {
        let wc = as_windowclause(l)?;
        if wc.name.is_none() {
            continue; // anonymous
        }
        if sep.is_none() {
            append_context_keyword(context, " WINDOW ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        } else {
            str_(context, sep.unwrap())?;
        }
        let q = quote_identifier(mcx, wc.name.as_deref().unwrap())?;
        str_(context, q.as_str())?;
        str_(context, " AS ")?;
        get_rule_windowspec(mcx, wc, &query.targetList, context)?;
        sep = Some(", ");
    }
    Ok(())
}

/// `get_rule_windowspec(wc, targetList, context)` (ruleutils.c 6780-6832).
/// **Public**: F1's WindowFunc OVER anonymous-window path reaches it.
pub fn get_rule_windowspec<'mcx>(
    mcx: Mcx<'mcx>,
    wc: &WindowClause<'mcx>,
    target_list: &[types_nodes::primnodes::TargetEntry<'mcx>],
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ch_(context, b'(')?;
    let mut needspace = false;
    if let Some(rn) = wc.refname.as_deref() {
        let q = quote_identifier(mcx, rn)?;
        str_(context, q.as_str())?;
        needspace = true;
    }
    // partition clauses inherited; print only if no refname
    if !wc.partitionClause.is_empty() && wc.refname.is_none() {
        if needspace {
            ch_(context, b' ')?;
        }
        str_(context, "PARTITION BY ")?;
        let mut sep = "";
        for l in wc.partitionClause.iter() {
            let grp = as_sortgroupclause(l)?;
            str_(context, sep)?;
            get_rule_sortgroupclause(mcx, grp.tleSortGroupRef, target_list, false, context)?;
            sep = ", ";
        }
        needspace = true;
    }
    if !wc.orderClause.is_empty() && !wc.copiedOrder {
        if needspace {
            ch_(context, b' ')?;
        }
        str_(context, "ORDER BY ")?;
        let order = node_vec_to_sgc(mcx, &wc.orderClause)?;
        get_rule_orderby(mcx, &order, target_list, false, context)?;
        needspace = true;
    }
    if wc.frameOptions & FRAMEOPTION_NONDEFAULT != 0 {
        if needspace {
            ch_(context, b' ')?;
        }
        get_window_frame_options(
            wc.frameOptions,
            wc.startOffset.as_deref(),
            wc.endOffset.as_deref(),
            context,
        )?;
    }
    ch_(context, b')')?;
    Ok(())
}

/// `get_window_frame_options(frameOptions, startOffset, endOffset, context)`
/// (ruleutils.c 6837-6901).
pub(crate) fn get_window_frame_options<'mcx>(
    frame_options: i32,
    start_offset: Option<&Node<'mcx>>,
    end_offset: Option<&Node<'mcx>>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if frame_options & FRAMEOPTION_NONDEFAULT == 0 {
        return Ok(());
    }
    if frame_options & FRAMEOPTION_RANGE != 0 {
        str_(context, "RANGE ")?;
    } else if frame_options & FRAMEOPTION_ROWS != 0 {
        str_(context, "ROWS ")?;
    } else if frame_options & FRAMEOPTION_GROUPS != 0 {
        str_(context, "GROUPS ")?;
    } else {
        return Err(elog_error("invalid frame mode".into()));
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        str_(context, "BETWEEN ")?;
    }
    if frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        str_(context, "UNBOUNDED PRECEDING ")?;
    } else if frame_options & FRAMEOPTION_START_CURRENT_ROW != 0 {
        str_(context, "CURRENT ROW ")?;
    } else if frame_options & FRAMEOPTION_START_OFFSET != 0 {
        let so = start_offset.ok_or_else(|| missing_field("frame startOffset"))?;
        get_rule_expr(so, context, false)?;
        if frame_options & FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
            str_(context, " PRECEDING ")?;
        } else if frame_options & FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
            str_(context, " FOLLOWING ")?;
        } else {
            return Err(elog_error("invalid frame start offset".into()));
        }
    } else {
        return Err(elog_error("invalid frame start".into()));
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        str_(context, "AND ")?;
        if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
            str_(context, "UNBOUNDED FOLLOWING ")?;
        } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
            str_(context, "CURRENT ROW ")?;
        } else if frame_options & FRAMEOPTION_END_OFFSET != 0 {
            let eo = end_offset.ok_or_else(|| missing_field("frame endOffset"))?;
            get_rule_expr(eo, context, false)?;
            if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                str_(context, " PRECEDING ")?;
            } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
                str_(context, " FOLLOWING ")?;
            } else {
                return Err(elog_error("invalid frame end offset".into()));
            }
        } else {
            return Err(elog_error("invalid frame end".into()));
        }
    }
    if frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        str_(context, "EXCLUDE CURRENT ROW ")?;
    } else if frame_options & FRAMEOPTION_EXCLUDE_GROUP != 0 {
        str_(context, "EXCLUDE GROUP ")?;
    } else if frame_options & FRAMEOPTION_EXCLUDE_TIES != 0 {
        str_(context, "EXCLUDE TIES ")?;
    }
    // remove the trailing space
    if !context.buf.data.is_empty() {
        context.buf.data.pop();
    }
    Ok(())
}

/// `get_window_frame_options_for_explain(frameOptions, startOffset, endOffset,
/// dpcontext, forceprefix)` (ruleutils.c 6907-6936). Builds a fresh
/// `deparse_context` over the supplied namespaces (already pointed at the
/// WindowAgg plan node by `set_deparse_context_plan`) and renders the frame
/// options text — the form EXPLAIN appends after a window's PARTITION/ORDER BY
/// keys. The offset expressions, if present, are deparsed against the plan
/// context (so OUTER_VAR/PARAM_EXEC references resolve).
pub(crate) fn get_window_frame_options_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    frame_options: i32,
    start_offset: Option<&Node<'mcx>>,
    end_offset: Option<&Node<'mcx>>,
    dpcontext: PgVec<'mcx, DeparseNamespace<'mcx>>,
    forceprefix: bool,
) -> PgResult<PgString<'mcx>> {
    let mut context = DeparseContext {
        buf: types_stringinfo::StringInfo::new_in(mcx),
        namespaces: dpcontext,
        resultDesc: None,
        targetList: PgVec::new_in(mcx),
        windowClause: PgVec::new_in(mcx),
        prettyFlags: 0,
        wrapColumn: crate::WRAP_COLUMN_DEFAULT,
        indentLevel: 0,
        varprefix: forceprefix,
        colNamesVisible: true,
        inGroupBy: false,
        varInOrderBy: false,
        appendparents: None,
    };

    get_window_frame_options(frame_options, start_offset, end_offset, &mut context)?;

    // return buf.data (palloc'd in C; here charged to mcx).
    let s = core::str::from_utf8(context.buf.data.as_slice())
        .map_err(|_| elog_error("deparse produced invalid UTF-8".into()))?;
    PgString::from_str_in(s, mcx)
}

/* -------------------------------------------------------------------------- *
 * INSERT / UPDATE / DELETE / MERGE / UTILITY (ruleutils.c 6938-7584).
 * -------------------------------------------------------------------------- */

/// `get_insert_query_def(query, context)` (ruleutils.c 6938-7143).
fn get_insert_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(mcx, query, context)?;

    // Find the single SELECT/VALUES RTE.
    let mut select_rte: Option<usize> = None;
    let mut values_rte: Option<usize> = None;
    for (i, rte) in query.rtable.iter().enumerate() {
        if rte.rtekind == RTE_SUBQUERY {
            if select_rte.is_some() {
                return Err(elog_error("too many subquery RTEs in INSERT".into()));
            }
            select_rte = Some(i);
        }
        if rte.rtekind == RTE_VALUES {
            if values_rte.is_some() {
                return Err(elog_error("too many values RTEs in INSERT".into()));
            }
            values_rte = Some(i);
        }
    }
    if select_rte.is_some() && values_rte.is_some() {
        return Err(elog_error("both subquery and values RTEs in INSERT".into()));
    }

    let rte = rt_fetch(query.resultRelation, &query.rtable)?;
    debug_assert_eq!(rte.rtekind, RTE_RELATION);
    let relid = rte.relid;

    if pretty_indent(context) {
        context.indentLevel += PRETTYINDENT_STD;
        ch_(context, b' ')?;
    }
    str_(context, "INSERT INTO ")?;
    let rn = generate_relation_name(mcx, relid, context)?;
    str_(context, rn.as_str())?;

    // Relation alias; INSERT requires explicit AS.
    let rte_clone = query.rtable[(query.resultRelation - 1) as usize].clone_in(mcx)?;
    get_rte_alias(mcx, &rte_clone, query.resultRelation, true, context)?;
    ch_(context, b' ')?;

    // Column-names list + collect stripped exprs.
    let mut stripped: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    let mut sep = "";
    if !query.targetList.is_empty() {
        ch_(context, b'(')?;
    }
    for tle in query.targetList.iter() {
        if tle.resjunk {
            continue;
        }
        str_(context, sep)?;
        sep = ", ";
        let an = get_attname(mcx, relid, tle.resno)?;
        let q = quote_identifier(mcx, an.as_str())?;
        str_(context, q.as_str())?;
        let expr = tle.expr.as_deref().ok_or_else(|| missing_field("INSERT TLE.expr"))?;
        let s = process_indirection(mcx, &Node::mk_expr(mcx, expr.clone_in(mcx)?)?, context)?;
        stripped.try_reserve(1).map_err(|_| mcx.oom(0))?;
        stripped.push(s);
    }
    if !query.targetList.is_empty() {
        str_(context, ") ")?;
    }

    match query.r#override {
        OverridingKind::OVERRIDING_SYSTEM_VALUE => str_(context, "OVERRIDING SYSTEM VALUE ")?,
        OverridingKind::OVERRIDING_USER_VALUE => str_(context, "OVERRIDING USER VALUE ")?,
        _ => {}
    }

    if let Some(ri) = select_rte {
        let subq = query.rtable[ri].subquery.as_deref().ok_or_else(|| missing_field("INSERT select subquery"))?;
        recurse_query_def(mcx, subq, context, None, false)?;
    } else if let Some(ri) = values_rte {
        get_values_def(&query.rtable[ri].values_lists, context)?;
    } else if !stripped.is_empty() {
        append_context_keyword(context, "VALUES (", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2)?;
        get_rule_list_toplevel(&stripped, context, false)?;
        ch_(context, b')')?;
    } else {
        str_(context, "DEFAULT VALUES")?;
    }

    // ON CONFLICT
    if let Some(confl) = query.onConflict.as_deref() {
        str_(context, " ON CONFLICT")?;
        if !confl.arbiterElems.is_empty() {
            ch_(context, b'(')?;
            let arb = node_list(mcx, &confl.arbiterElems)?;
            get_rule_expr(&arb, context, false)?;
            ch_(context, b')')?;
            if let Some(aw) = confl.arbiterWhere.as_deref() {
                let save = context.varprefix;
                context.varprefix = false;
                append_context_keyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
                get_rule_expr(aw, context, false)?;
                context.varprefix = save;
            }
        } else if confl.constraint != 0 {
            let name = backend_utils_cache_lsyscache_seams::get_constraint_name::call(mcx, confl.constraint)?
                .ok_or_else(|| elog_error(format!("cache lookup failed for constraint {}", confl.constraint)))?;
            str_(context, " ON CONSTRAINT ")?;
            let q = quote_identifier(mcx, name.as_str())?;
            str_(context, q.as_str())?;
        }

        if confl.action == OnConflictAction::ONCONFLICT_NOTHING {
            str_(context, " DO NOTHING")?;
        } else {
            str_(context, " DO UPDATE SET ")?;
            get_update_query_targetlist_def(mcx, query, &confl.onConflictSet, context, relid)?;
            if let Some(ow) = confl.onConflictWhere.as_deref() {
                append_context_keyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
                get_rule_expr(ow, context, false)?;
            }
        }
    }

    get_returning_clause(mcx, query, context)
}

/// `get_update_query_def(query, context)` (ruleutils.c 7150-7195).
fn get_update_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(mcx, query, context)?;

    let rte = rt_fetch(query.resultRelation, &query.rtable)?;
    debug_assert_eq!(rte.rtekind, RTE_RELATION);
    let relid = rte.relid;
    let only = only_marker(rte);

    if pretty_indent(context) {
        ch_(context, b' ')?;
        context.indentLevel += PRETTYINDENT_STD;
    }
    str_(context, "UPDATE ")?;
    str_(context, only)?;
    let rn = generate_relation_name(mcx, relid, context)?;
    str_(context, rn.as_str())?;

    let rte_clone = query.rtable[(query.resultRelation - 1) as usize].clone_in(mcx)?;
    get_rte_alias(mcx, &rte_clone, query.resultRelation, false, context)?;
    str_(context, " SET ")?;

    let tl = clone_node_list_from_tles(mcx, &query.targetList)?;
    get_update_query_targetlist_def(mcx, query, &tl, context, relid)?;

    get_from_clause(mcx, query, " FROM ", context)?;

    let jointree = query.jointree.as_deref().ok_or_else(|| missing_field("UPDATE jointree"))?;
    if let Some(quals) = jointree.quals.as_deref() {
        append_context_keyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        get_rule_expr(quals, context, false)?;
    }

    get_returning_clause(mcx, query, context)
}

/// `get_update_query_targetlist_def(query, targetList, context, rte)`
/// (ruleutils.c 7202-7347).
///
/// The MULTIEXPR multiassignment grouping (`(a, b) = (SELECT ...)`) is rendered
/// in full; it reaches `processIndirection` (plain target columns) and the F1
/// SubLink deparser for the closing sublink.
fn get_update_query_targetlist_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    target_list: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    context: &mut DeparseContext<'mcx>,
    relid: Oid,
) -> PgResult<()> {
    // Collect MULTIEXPR source SubLinks (in ID order, from resjunk tlist).
    let mut ma_sublinks: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    if query.hasSubLinks {
        for l in target_list.iter() {
            let tle = as_targetentry(l)?;
            if tle.resjunk {
                if let Some(e @ Expr::SubLink(sl)) = tle.expr.as_deref() {
                    if sl.subLinkType == types_nodes::primnodes::SubLinkType::MultiExpr {
                        ma_sublinks.try_reserve(1).map_err(|_| mcx.oom(0))?;
                        ma_sublinks.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e.clone_in(mcx)?)?)?);
                    }
                }
            }
        }
    }
    let mut next_ma = 0usize;
    let mut cur_ma_active = false;
    let mut cur_ma_sublink: Option<PgBox<'mcx, Node<'mcx>>> = None;
    let mut remaining_ma = 0i32;

    let mut sep = "";
    for l in target_list.iter() {
        let tle = as_targetentry(l)?;
        if tle.resjunk {
            continue;
        }
        str_(context, sep)?;
        sep = ", ";

        // Multiassignment group start detection.
        if next_ma < ma_sublinks.len() && !cur_ma_active {
            // Dig down past FieldStore/SubscriptingRef/CoerceToDomain.
            if expr_is_multiexpr_param(tle.expr.as_deref()) {
                cur_ma_sublink = Some(mcx::alloc_in(mcx, ma_sublinks[next_ma].clone_in(mcx)?)?);
                next_ma += 1;
                cur_ma_active = true;
                remaining_ma = multiexpr_remaining(cur_ma_sublink.as_deref())?;
                ch_(context, b'(')?;
            }
        }

        // Column name.
        let an = get_attname(mcx, relid, tle.resno)?;
        let q = quote_identifier(mcx, an.as_str())?;
        str_(context, q.as_str())?;

        // Indirection.
        let expr = tle.expr.as_deref().ok_or_else(|| missing_field("UPDATE TLE.expr"))?;
        let mut emitted = process_indirection(mcx, &Node::mk_expr(mcx, expr.clone_in(mcx)?)?, context)?;

        // Multiassignment skip-until-last.
        if cur_ma_active {
            remaining_ma -= 1;
            if remaining_ma > 0 {
                continue;
            }
            ch_(context, b')')?;
            emitted = cur_ma_sublink.take().unwrap();
            cur_ma_active = false;
        }

        str_(context, " = ")?;
        get_rule_expr(&emitted, context, false)?;
    }
    Ok(())
}

/// `get_delete_query_def(query, context)` (ruleutils.c 7354-7394).
fn get_delete_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(mcx, query, context)?;

    let rte = rt_fetch(query.resultRelation, &query.rtable)?;
    debug_assert_eq!(rte.rtekind, RTE_RELATION);
    let relid = rte.relid;
    let only = only_marker(rte);

    if pretty_indent(context) {
        ch_(context, b' ')?;
        context.indentLevel += PRETTYINDENT_STD;
    }
    str_(context, "DELETE FROM ")?;
    str_(context, only)?;
    let rn = generate_relation_name(mcx, relid, context)?;
    str_(context, rn.as_str())?;

    let rte_clone = query.rtable[(query.resultRelation - 1) as usize].clone_in(mcx)?;
    get_rte_alias(mcx, &rte_clone, query.resultRelation, false, context)?;

    get_from_clause(mcx, query, " USING ", context)?;

    let jointree = query.jointree.as_deref().ok_or_else(|| missing_field("DELETE jointree"))?;
    if let Some(quals) = jointree.quals.as_deref() {
        append_context_keyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)?;
        get_rule_expr(quals, context, false)?;
    }

    get_returning_clause(mcx, query, context)
}

/// `get_merge_query_def(query, context)` (ruleutils.c 7401-7553).
fn get_merge_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(mcx, query, context)?;

    let rte = rt_fetch(query.resultRelation, &query.rtable)?;
    debug_assert_eq!(rte.rtekind, RTE_RELATION);
    let relid = rte.relid;

    if pretty_indent(context) {
        ch_(context, b' ')?;
        context.indentLevel += PRETTYINDENT_STD;
    }
    str_(context, "MERGE INTO ")?;
    let rn = generate_relation_name(mcx, relid, context)?;
    str_(context, rn.as_str())?;

    let rte_clone = query.rtable[(query.resultRelation - 1) as usize].clone_in(mcx)?;
    get_rte_alias(mcx, &rte_clone, query.resultRelation, false, context)?;

    get_from_clause(mcx, query, " USING ", context)?;
    append_context_keyword(context, " ON ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2)?;
    let mjc = query.mergeJoinCondition.as_deref().ok_or_else(|| missing_field("MERGE mergeJoinCondition"))?;
    get_rule_expr(&Node::mk_expr(mcx, mjc.clone_in(mcx)?)?, context, false)?;

    // any NOT MATCHED BY SOURCE?
    let mut have_nmbs = false;
    for lc in query.mergeActionList.iter() {
        let action = as_mergeaction(lc)?;
        if action.matchKind == MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE {
            have_nmbs = true;
            break;
        }
    }

    for lc in query.mergeActionList.iter() {
        let action = as_mergeaction(lc)?;
        append_context_keyword(context, " WHEN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2)?;
        match action.matchKind {
            MergeMatchKind::MERGE_WHEN_MATCHED => str_(context, "MATCHED")?,
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE => str_(context, "NOT MATCHED BY SOURCE")?,
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET => {
                if have_nmbs {
                    str_(context, "NOT MATCHED BY TARGET")?;
                } else {
                    str_(context, "NOT MATCHED")?;
                }
            }
        }

        if let Some(qual) = action.qual.as_deref() {
            append_context_keyword(context, " AND ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 3)?;
            get_rule_expr(qual, context, false)?;
        }
        append_context_keyword(context, " THEN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 3)?;

        match action.commandType {
            CmdType::CMD_INSERT => {
                let mut stripped: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
                let mut isep = "";
                str_(context, "INSERT")?;
                if !action.targetList.is_empty() {
                    str_(context, " (")?;
                }
                for lc2 in action.targetList.iter() {
                    let tle = as_targetentry(lc2)?;
                    debug_assert!(!tle.resjunk);
                    str_(context, isep)?;
                    isep = ", ";
                    let an = get_attname(mcx, relid, tle.resno)?;
                    let q = quote_identifier(mcx, an.as_str())?;
                    str_(context, q.as_str())?;
                    let expr = tle.expr.as_deref().ok_or_else(|| missing_field("MERGE INSERT TLE.expr"))?;
                    let s = process_indirection(mcx, &Node::mk_expr(mcx, expr.clone_in(mcx)?)?, context)?;
                    stripped.try_reserve(1).map_err(|_| mcx.oom(0))?;
                    stripped.push(s);
                }
                if !action.targetList.is_empty() {
                    ch_(context, b')')?;
                }
                match action.r#override {
                    OverridingKind::OVERRIDING_SYSTEM_VALUE => str_(context, " OVERRIDING SYSTEM VALUE")?,
                    OverridingKind::OVERRIDING_USER_VALUE => str_(context, " OVERRIDING USER VALUE")?,
                    _ => {}
                }
                if !stripped.is_empty() {
                    append_context_keyword(context, " VALUES (", -PRETTYINDENT_STD, PRETTYINDENT_STD, 4)?;
                    get_rule_list_toplevel(&stripped, context, false)?;
                    ch_(context, b')')?;
                } else {
                    str_(context, " DEFAULT VALUES")?;
                }
            }
            CmdType::CMD_UPDATE => {
                str_(context, "UPDATE SET ")?;
                get_update_query_targetlist_def(mcx, query, &action.targetList, context, relid)?;
            }
            CmdType::CMD_DELETE => str_(context, "DELETE")?,
            CmdType::CMD_NOTHING => str_(context, "DO NOTHING")?,
            other => {
                return Err(elog_error(format!(
                    "unrecognized MERGE action commandType: {}",
                    other as i32
                )))
            }
        }
    }

    get_returning_clause(mcx, query, context)
}

/// `get_utility_query_def(query, context)` (ruleutils.c 7560-7584).
fn get_utility_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match query.utilityStmt.as_deref().map(|n| (n.node_tag(), n)) {
        Some((types_nodes::nodes::ntag::T_NotifyStmt, n)) => {
            let stmt = n.expect_notifystmt();
            append_context_keyword(context, "", 0, PRETTYINDENT_STD, 1)?;
            str_(context, "NOTIFY ")?;
            let cn = stmt.conditionname.as_deref().ok_or_else(|| missing_field("NotifyStmt.conditionname"))?;
            let q = quote_identifier(mcx, cn)?;
            str_(context, q.as_str())?;
            if let Some(payload) = stmt.payload.as_deref() {
                str_(context, ", ")?;
                crate::simple_quote_literal_pub(context, payload)?;
            }
            Ok(())
        }
        _ => Err(elog_error("unexpected utility statement type".into())),
    }
}

/* -------------------------------------------------------------------------- *
 * FROM clause (ruleutils.c 12269-12849).
 * -------------------------------------------------------------------------- */

/// `get_from_clause(query, prefix, context)` (ruleutils.c 12269-12361).
fn get_from_clause<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    prefix: &str,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let jointree = query.jointree.as_deref().ok_or_else(|| missing_field("jointree"))?;
    let mut first = true;
    for l in jointree.fromlist.iter() {
        // Skip auto-added not-inFromCl RTEs at top level.
        if let Some(rtr) = (**l).as_rangetblref() {
            let rte = rt_fetch(rtr.rtindex, &query.rtable)?;
            if !rte.inFromCl {
                continue;
            }
        }

        if first {
            append_context_keyword(context, prefix, -PRETTYINDENT_STD, PRETTYINDENT_STD, 2)?;
            first = false;
            get_from_clause_item(mcx, l, query, context)?;
        } else {
            str_(context, ", ")?;
            // itembuf for line-wrapping
            let saved = core::mem::replace(&mut context.buf, types_stringinfo::StringInfo::new_in(mcx));
            get_from_clause_item(mcx, l, query, context)?;
            let itembuf = core::mem::replace(&mut context.buf, saved);

            if pretty_indent(context) && context.wrapColumn >= 0 {
                if !itembuf.data.is_empty() && itembuf.data[0] == b'\n' {
                    remove_string_info_spaces(context);
                } else {
                    let trailing = trailing_line_len(&context.buf.data);
                    if trailing + itembuf.data.len() > context.wrapColumn as usize {
                        append_context_keyword(context, "", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_VAR)?;
                    }
                }
            }
            append_binary(context, &itembuf.data)?;
        }
    }
    Ok(())
}

/// `get_from_clause_item(jtnode, query, context)` (ruleutils.c 12363-12647).
fn get_from_clause_item<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &Node<'mcx>,
    query: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match jtnode.node_tag() {
        types_nodes::nodes::ntag::T_RangeTblRef => {
            let rtr = jtnode.expect_rangetblref();
            let varno = rtr.rtindex;
            let rte = rt_fetch(varno, &query.rtable)?.clone_in(mcx)?;
            let mut rtfunc1_present = false;

            if rte.lateral {
                str_(context, "LATERAL ")?;
            }

            match rte.rtekind {
                RTE_RELATION => {
                    str_(context, only_marker(&rte))?;
                    let rn = generate_relation_name(mcx, rte.relid, context)?;
                    str_(context, rn.as_str())?;
                }
                RTE_SUBQUERY => {
                    ch_(context, b'(')?;
                    let subq = rte.subquery.as_deref().ok_or_else(|| missing_field("FROM subquery"))?;
                    recurse_query_def(mcx, subq, context, None, true)?;
                    ch_(context, b')')?;
                }
                RTE_FUNCTION => {
                    get_function_rte(mcx, &rte, context, &mut rtfunc1_present)?;
                }
                RTE_TABLEFUNC => {
                    // C: get_tablefunc(rte->tablefunc, context, true).
                    let tf = rte
                        .tablefunc
                        .as_deref()
                        .and_then(|n| n.as_table_func())
                        .ok_or_else(|| missing_field("RTE_TABLEFUNC tablefunc"))?;
                    crate::expr_deparse::get_tablefunc(tf, context, true)?;
                }
                RTE_VALUES => {
                    ch_(context, b'(')?;
                    get_values_def(&rte.values_lists, context)?;
                    ch_(context, b')')?;
                }
                RTE_CTE => {
                    let cn = rte.ctename.as_deref().ok_or_else(|| missing_field("CTE RTE ctename"))?;
                    let q = quote_identifier(mcx, cn)?;
                    str_(context, q.as_str())?;
                }
                other => {
                    return Err(elog_error(format!("unrecognized RTE kind: {}", other as i32)))
                }
            }

            // Relation alias.
            get_rte_alias(mcx, &rte, varno, false, context)?;

            // Column defs / aliases.
            let dpns = &context.namespaces[0];
            let has_funccolnames = rtfunc1_present && {
                let rtfunc1 = first_rtfunc(&rte)?;
                rtfunc1.map(|r| !r.funccolnames.is_empty()).unwrap_or(false)
            };
            let _ = dpns;
            if has_funccolnames {
                let colinfo = deparse_columns_fetch(varno, &context.namespaces[0]).clone_columns(mcx)?;
                let rtfunc1 = first_rtfunc(&rte)?.unwrap();
                let rtf = rtfunc1.clone_in(mcx)?;
                get_from_clause_coldeflist(mcx, &rtf, Some(&colinfo), context)?;
            } else {
                let colinfo = deparse_columns_fetch(varno, &context.namespaces[0]).clone_columns(mcx)?;
                get_column_alias_list(mcx, &colinfo, context)?;
            }

            // Tablesample.
            if rte.rtekind == RTE_RELATION {
                if let Some(ts) = rte.tablesample.as_deref() {
                    if let Some(t) = ts.as_tablesampleclause() {
                        get_tablesample_def(mcx, t, context)?;
                    }
                }
            }
            Ok(())
        }
        types_nodes::nodes::ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            let colinfo = deparse_columns_fetch(j.rtindex, &context.namespaces[0]).clone_columns(mcx)?;

            let need_paren_on_right = pretty_paren(context)
                && !j.rarg.as_deref().is_some_and(|r| r.is_rangetblref())
                && !j.rarg.as_deref().is_some_and(|r| r.as_joinexpr().is_some_and(|jr| jr.alias.is_some()));

            if !pretty_paren(context) || j.alias.is_some() {
                ch_(context, b'(')?;
            }

            let larg = j.larg.as_deref().ok_or_else(|| missing_field("JoinExpr.larg"))?;
            let larg = larg.clone_in(mcx)?;
            get_from_clause_item(mcx, &larg, query, context)?;

            match j.jointype {
                types_nodes::jointype::JoinType::JOIN_INNER => {
                    if j.quals.is_some() {
                        append_context_keyword(context, " JOIN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_JOIN)?;
                    } else {
                        append_context_keyword(context, " CROSS JOIN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_JOIN)?;
                    }
                }
                types_nodes::jointype::JoinType::JOIN_LEFT => {
                    append_context_keyword(context, " LEFT JOIN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_JOIN)?;
                }
                types_nodes::jointype::JoinType::JOIN_FULL => {
                    append_context_keyword(context, " FULL JOIN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_JOIN)?;
                }
                types_nodes::jointype::JoinType::JOIN_RIGHT => {
                    append_context_keyword(context, " RIGHT JOIN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_JOIN)?;
                }
                other => {
                    return Err(elog_error(format!("unrecognized join type: {}", other as i32)))
                }
            }

            if need_paren_on_right {
                ch_(context, b'(')?;
            }
            let rarg = j.rarg.as_deref().ok_or_else(|| missing_field("JoinExpr.rarg"))?;
            let rarg = rarg.clone_in(mcx)?;
            get_from_clause_item(mcx, &rarg, query, context)?;
            if need_paren_on_right {
                ch_(context, b')')?;
            }

            if !j.usingClause.is_empty() {
                str_(context, " USING (")?;
                let mut first = true;
                for colname in colinfo.usingNames.iter() {
                    if first {
                        first = false;
                    } else {
                        str_(context, ", ")?;
                    }
                    let q = quote_identifier(mcx, colname.as_str())?;
                    str_(context, q.as_str())?;
                }
                ch_(context, b')')?;
                if let Some(jua) = j.join_using_alias.as_ref() {
                    str_(context, " AS ")?;
                    let q = quote_identifier(mcx, jua.aliasname.as_deref().unwrap_or(""))?;
                    str_(context, q.as_str())?;
                }
            } else if let Some(quals) = j.quals.as_deref() {
                str_(context, " ON ")?;
                if !pretty_paren(context) {
                    ch_(context, b'(')?;
                }
                get_rule_expr(quals, context, false)?;
                if !pretty_paren(context) {
                    ch_(context, b')')?;
                }
            } else if j.jointype != types_nodes::jointype::JoinType::JOIN_INNER {
                str_(context, " ON TRUE")?;
            }

            if !pretty_paren(context) || j.alias.is_some() {
                ch_(context, b')')?;
            }

            if j.alias.is_some() {
                ch_(context, b' ')?;
                let name = crate::get_rtable_name_pub(j.rtindex, context)?;
                let q = quote_identifier(mcx, name.unwrap_or(""))?;
                str_(context, q.as_str())?;
                get_column_alias_list(mcx, &colinfo, context)?;
            }
            Ok(())
        }
        _ => Err(elog_error(format!("unrecognized node type: {}", jtnode.tag().0))),
    }
}

/// The RTE_FUNCTION arm of `get_from_clause_item` (ruleutils.c 12398-12492).
fn get_function_rte<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    context: &mut DeparseContext<'mcx>,
    rtfunc1_present: &mut bool,
) -> PgResult<()> {
    let rtfunc1 = first_rtfunc(rte)?.ok_or_else(|| missing_field("RTE_FUNCTION functions[0]"))?;
    *rtfunc1_present = true;

    if rte.functions.len() == 1 && (rtfunc1.funccolnames.is_empty() || !rte.funcordinality) {
        let fe = rtfunc1.funcexpr.as_deref().ok_or_else(|| missing_field("RangeTblFunction.funcexpr"))?;
        get_rule_expr_funccall(fe, context, true)?;
    } else {
        // Check all-unnest collapse.
        let mut all_unnest = true;
        for lc in rte.functions.iter() {
            let rtfunc = as_rtfunc(lc)?;
            let is_unnest = match rtfunc.funcexpr.as_deref().and_then(|n| n.as_expr()) {
                Some(Expr::FuncExpr(f)) => f.funcid == F_UNNEST_ANYARRAY,
                _ => false,
            };
            if !is_unnest || !rtfunc.funccolnames.is_empty() {
                all_unnest = false;
                break;
            }
        }

        if all_unnest {
            let mut allargs: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
            for lc in rte.functions.iter() {
                let rtfunc = as_rtfunc(lc)?;
                if let Some(f) = rtfunc.funcexpr.as_deref().and_then(|n| n.as_funcexpr()) {
                    for a in f.args.iter() {
                        allargs.try_reserve(1).map_err(|_| mcx.oom(0))?;
                        allargs.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, a.clone_in(mcx)?)?)?);
                    }
                }
            }
            str_(context, "UNNEST(")?;
            let list = mcx::alloc_in(mcx, Node::mk_list(mcx, allargs)?)?;
            get_rule_expr(&list, context, true)?;
            ch_(context, b')')?;
        } else {
            str_(context, "ROWS FROM(")?;
            let mut funcno = 0;
            for lc in rte.functions.iter() {
                let rtfunc = as_rtfunc(lc)?;
                if funcno > 0 {
                    str_(context, ", ")?;
                }
                let fe = rtfunc.funcexpr.as_deref().ok_or_else(|| missing_field("rtfunc.funcexpr"))?;
                get_rule_expr_funccall(fe, context, true)?;
                if !rtfunc.funccolnames.is_empty() {
                    str_(context, " AS ")?;
                    let rtf = rtfunc.clone_in(mcx)?;
                    get_from_clause_coldeflist(mcx, &rtf, None, context)?;
                }
                funcno += 1;
            }
            ch_(context, b')')?;
        }
        // suppress the coldeflist below
        *rtfunc1_present = false;
    }
    if rte.funcordinality {
        str_(context, " WITH ORDINALITY")?;
    }
    Ok(())
}

/// `get_rte_alias(rte, varno, use_as, context)` (ruleutils.c 12654-12718).
fn get_rte_alias<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    varno: i32,
    use_as: bool,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let refname = crate::get_rtable_name_pub(varno, context)?.map(String::from);
    let printaliases = deparse_columns_fetch(varno, &context.namespaces[0]).printaliases;

    let mut printalias = false;
    if rte.alias.is_some() {
        printalias = true;
    } else if printaliases {
        printalias = true;
    } else if rte.rtekind == RTE_RELATION {
        let actual = generate_relation_name_raw(mcx, rte.relid)?;
        if refname.as_deref() != Some(actual.as_str()) {
            printalias = true;
        }
    } else if rte.rtekind == RTE_FUNCTION {
        printalias = true;
    } else if rte.rtekind == RTE_SUBQUERY || rte.rtekind == RTE_VALUES {
        printalias = true;
    } else if rte.rtekind == RTE_CTE {
        if refname.as_deref() != rte.ctename.as_deref() {
            printalias = true;
        }
    }

    if printalias {
        str_(context, if use_as { " AS " } else { " " })?;
        let q = quote_identifier(mcx, refname.as_deref().unwrap_or(""))?;
        str_(context, q.as_str())?;
    }
    Ok(())
}

/// `get_column_alias_list(colinfo, context)` (ruleutils.c 12725-12751).
fn get_column_alias_list<'mcx>(
    mcx: Mcx<'mcx>,
    colinfo: &DeparseColumns<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if !colinfo.printaliases {
        return Ok(());
    }
    let mut first = true;
    for i in 0..(colinfo.num_new_cols as usize) {
        let colname = colinfo.new_colnames[i].as_deref().unwrap_or("");
        if first {
            ch_(context, b'(')?;
            first = false;
        } else {
            str_(context, ", ")?;
        }
        let q = quote_identifier(mcx, colname)?;
        str_(context, q.as_str())?;
    }
    if !first {
        ch_(context, b')')?;
    }
    Ok(())
}

/// `get_from_clause_coldeflist(rtfunc, colinfo, context)` (ruleutils.c
/// 12765-12811).
fn get_from_clause_coldeflist<'mcx>(
    mcx: Mcx<'mcx>,
    rtfunc: &RangeTblFunction<'mcx>,
    colinfo: Option<&DeparseColumns<'mcx>>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ch_(context, b'(')?;
    let n = rtfunc.funccoltypes.len();
    for i in 0..n {
        let atttypid = rtfunc.funccoltypes[i];
        let atttypmod = rtfunc.funccoltypmods[i];
        let attcollation = rtfunc.funccolcollations[i];
        let attname: PgString<'mcx> = if let Some(ci) = colinfo {
            PgString::from_str_in(ci.colnames[i].as_deref().unwrap_or(""), mcx)?
        } else {
            PgString::from_str_in(crate::str_val_pub(&rtfunc.funccolnames[i])?, mcx)?
        };

        if i > 0 {
            str_(context, ", ")?;
        }
        let q = quote_identifier(mcx, attname.as_str())?;
        str_(context, q.as_str())?;
        ch_(context, b' ')?;
        let ty = crate::format_type_with_typemod_pub(mcx, atttypid, atttypmod)?;
        str_(context, ty.as_str())?;
        if attcollation != 0 {
            let typcoll = backend_utils_cache_lsyscache_seams::get_typcollation::call(atttypid)?;
            if attcollation != typcoll {
                str_(context, " COLLATE ")?;
                let cn = generate_collation_name(mcx, attcollation)?;
                str_(context, cn.as_str())?;
            }
        }
    }
    ch_(context, b')')?;
    Ok(())
}

/// `get_tablesample_def(tablesample, context)` (ruleutils.c 12816-12849).
fn get_tablesample_def<'mcx>(
    mcx: Mcx<'mcx>,
    tablesample: &types_nodes::nodesamplescan::TableSampleClause<'mcx>,
    context: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut argtypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    argtypes.try_reserve(1).map_err(|_| mcx.oom(0))?;
    argtypes.push(INTERNALOID);

    str_(context, " TABLESAMPLE ")?;
    let (name, _uv) = backend_utils_adt_ruleutils_seams::generate_function_name::call(
        mcx,
        tablesample.tsmhandler,
        1,
        PgVec::new_in(mcx),
        argtypes,
        false,
        false,
        false,
    )?;
    str_(context, name.as_str())?;
    str_(context, " (")?;

    let mut nargs = 0;
    if let Some(args) = tablesample.args.as_ref() {
        for a in args.iter() {
            if nargs > 0 {
                str_(context, ", ")?;
            }
            nargs += 1;
            get_rule_expr(&Node::mk_expr(mcx, a.clone_in(mcx)?)?, context, false)?;
        }
    }
    ch_(context, b')')?;

    if let Some(rep) = tablesample.repeatable.as_deref() {
        str_(context, " REPEATABLE (")?;
        get_rule_expr(&Node::mk_expr(mcx, rep.clone_in(mcx)?)?, context, false)?;
        ch_(context, b')')?;
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * processIndirection (ruleutils.c 12920-12996).
 * -------------------------------------------------------------------------- */

/// `processIndirection(node, context)` (ruleutils.c 12920-12996).
///
/// Strips top-level FieldStore / assignment-SubscriptingRef / implicit
/// CoerceToDomain decoration, printing it after the base column. The plain
/// (no-indirection) target column — the dominant case — returns the node
/// unchanged with no output; actual FieldStore / assignment SubsRef reach
/// `get_typ_typrelid` / `get_attname` / `printSubscripts` (the F1 assignment
/// path), which are a later family, so they panic precisely.
fn process_indirection<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    _context: &mut DeparseContext<'mcx>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // Detect whether any indirection is present at the top.
    if let Some(e) = node.as_expr() {
        match e {
            Expr::FieldStore(_) => {
                return Err(deferred(
                    "processIndirection FieldStore (get_typ_typrelid/get_attname field decoration; assignment family)",
                ));
            }
            Expr::SubscriptingRef(s) if s.refassgnexpr.is_some() => {
                return Err(deferred(
                    "processIndirection assignment SubscriptingRef (printSubscripts assignment path; assignment family)",
                ));
            }
            Expr::CoerceToDomain(_) => {
                // C may descend past an implicit CoerceToDomain to find an
                // assignment node below it; reaching that requires the same
                // FieldStore/SubsRef handling.
                return Err(deferred(
                    "processIndirection CoerceToDomain descent (assignment-node indirection; assignment family)",
                ));
            }
            _ => {}
        }
    }
    // No indirection: return the node unchanged.
    Ok(mcx::alloc_in(mcx, node.clone_in(mcx)?)?)
}

/* -------------------------------------------------------------------------- *
 * Small node-access + clone helpers.
 * -------------------------------------------------------------------------- */

/// `get_sortgroupref_tle(sortref, targetList)` (optimizer/util/tlist.c
/// 349-368): the TargetEntry whose `ressortgroupref == sortref`.
fn get_sortgroupref_tle<'a, 'mcx>(
    sortref: u32,
    target_list: &'a [types_nodes::primnodes::TargetEntry<'mcx>],
) -> PgResult<&'a types_nodes::primnodes::TargetEntry<'mcx>> {
    for tle in target_list.iter() {
        if tle.ressortgroupref == sortref {
            return Ok(tle);
        }
    }
    Err(elog_error(format!("ORDER/GROUP BY expression not found in targetlist (ref {sortref})")))
}

/// Convert a `List *` of `SortGroupClause` (stored as `Node::SortGroupClause`)
/// into a `Vec<SortGroupClause>` (Copy) — the form `get_rule_orderby` consumes.
fn node_vec_to_sgc<'mcx>(
    mcx: Mcx<'mcx>,
    v: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<alloc::vec::Vec<SortGroupClause>> {
    let mut out = alloc::vec::Vec::new();
    out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
    for n in v.iter() {
        out.push(*as_sortgroupclause(n)?);
    }
    Ok(out)
}

fn as_sortgroupclause<'a>(node: &'a Node<'_>) -> PgResult<&'a SortGroupClause> {
    node.as_sortgroupclause()
        .ok_or_else(|| elog_error(format!("expected SortGroupClause, got tag {}", node.tag().0)))
}

fn as_groupingset<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a GroupingSet<'mcx>> {
    node.as_groupingset()
        .ok_or_else(|| elog_error(format!("expected GroupingSet, got tag {}", node.tag().0)))
}

fn as_windowclause<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a WindowClause<'mcx>> {
    node.as_windowclause()
        .ok_or_else(|| elog_error(format!("expected WindowClause, got tag {}", node.tag().0)))
}

fn as_targetentry<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a types_nodes::primnodes::TargetEntry<'mcx>> {
    match node.node_tag() {
        types_nodes::nodes::ntag::T_TargetEntry => Ok(node.expect_targetentry()),
        _ => Err(elog_error(format!("expected TargetEntry, got tag {}", node.tag().0))),
    }
}

fn as_mergeaction<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a types_nodes::rawnodes::MergeAction<'mcx>> {
    node.as_mergeaction()
        .ok_or_else(|| elog_error(format!("expected MergeAction, got tag {}", node.tag().0)))
}

fn as_rtfunc<'a, 'mcx>(node: &'a Node<'mcx>) -> PgResult<&'a RangeTblFunction<'mcx>> {
    node.as_rangetblfunction()
        .ok_or_else(|| elog_error(format!("expected RangeTblFunction, got tag {}", node.tag().0)))
}

fn node_as_int(node: &Node<'_>) -> PgResult<i32> {
    node.as_integer()
        .map(|i| i.ival)
        .ok_or_else(|| elog_error(format!("expected Integer, got tag {}", node.tag().0)))
}

fn first_rtfunc<'a, 'mcx>(rte: &'a RangeTblEntry<'mcx>) -> PgResult<Option<&'a RangeTblFunction<'mcx>>> {
    match rte.functions.first() {
        Some(n) => Ok(Some(as_rtfunc(n)?)),
        None => Ok(None),
    }
}

/// `NameStr(TupleDescAttr(rd, idx)->attname)`.
fn tupdesc_attname(
    rd: &types_tuple::heaptuple::TupleDescData<'_>,
    idx: usize,
) -> PgResult<String> {
    let attr = rd.attrs.get(idx).ok_or_else(|| elog_error(format!("tupdesc attr {idx} out of range")))?;
    Ok(String::from_utf8_lossy(attr.attname.name_str()).into_owned())
}

fn clone_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    tl: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(tl.len()).map_err(|_| mcx.oom(0))?;
    for t in tl.iter() {
        out.push(t.clone_in(mcx)?);
    }
    Ok(out)
}

fn clone_window_clauses<'mcx>(
    mcx: Mcx<'mcx>,
    wcs: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<PgVec<'mcx, WindowClause<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(wcs.len()).map_err(|_| mcx.oom(0))?;
    for n in wcs.iter() {
        out.push(as_windowclause(n)?.clone_in(mcx)?);
    }
    Ok(out)
}

fn clone_node_list_from_tles<'mcx>(
    mcx: Mcx<'mcx>,
    tl: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(tl.len()).map_err(|_| mcx.oom(0))?;
    for t in tl.iter() {
        out.push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, t.clone_in(mcx)?)?)?);
    }
    Ok(out)
}

fn node_list<'mcx>(
    mcx: Mcx<'mcx>,
    items: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<Node<'mcx>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(items.len()).map_err(|_| mcx.oom(0))?;
    for n in items.iter() {
        out.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(Node::mk_list(mcx, out)?)
}

fn clone_opt_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    rd: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>> {
    match rd {
        Some(d) => Ok(Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `generate_relation_name_raw(relid)` — the unqualified live relation name
/// (`get_relation_name`, ruleutils.c 13133), used by `get_rte_alias` to compare
/// against the chosen refname. Routed through `get_rel_name` (lsyscache).
fn generate_relation_name_raw<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgString<'mcx>> {
    match crate::get_rel_name_pub(mcx, relid)? {
        Some(s) => Ok(s),
        None => Err(elog_error(format!("cache lookup failed for relation {}", relid))),
    }
}

/// Recurse into a sub-`Query` via `get_query_def`, swapping the output buffer in
/// and out of `context` (C threads the same StringInfo through the recursion).
fn recurse_query_def<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &Query<'mcx>,
    context: &mut DeparseContext<'mcx>,
    result_desc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    col_names_visible: bool,
) -> PgResult<()> {
    // Take the buffer out, run get_query_def with the current namespaces as
    // parent namespaces, then put the buffer back.
    let buf = core::mem::replace(&mut context.buf, types_stringinfo::StringInfo::new_in(mcx));
    let parent: PgVec<'mcx, DeparseNamespace<'mcx>> = {
        let mut v = PgVec::new_in(mcx);
        v.try_reserve(context.namespaces.len()).map_err(|_| mcx.oom(0))?;
        for ns in context.namespaces.iter() {
            v.push(crate::clone_namespace_pub(mcx, ns)?);
        }
        v
    };
    let out = get_query_def(
        mcx,
        subquery,
        buf,
        &parent,
        result_desc,
        col_names_visible,
        context.prettyFlags,
        context.wrapColumn,
        context.indentLevel,
    )?;
    context.buf = out;
    Ok(())
}

/// `appendBinaryStringInfo(buf, data, len)`.
fn append_binary(context: &mut DeparseContext<'_>, data: &[u8]) -> PgResult<()> {
    let mcx = context.buf.allocator();
    context.buf.data.try_reserve(data.len()).map_err(|_| mcx.oom(data.len()))?;
    context.buf.data.extend_from_slice(data);
    Ok(())
}

/// The byte length of the current (last) line in `data` (everything after the
/// final '\n', or the whole thing if none).
fn trailing_line_len(data: &[u8]) -> usize {
    match data.iter().rposition(|&b| b == b'\n') {
        Some(p) => data.len() - p - 1,
        None => data.len(),
    }
}

/// Whether `tle.expr` digs down (past FieldStore/SubsRef/implicit CoerceToDomain
/// and implicit coercions) to a `PARAM_MULTIEXPR` Param (ruleutils.c 7271-7303).
/// The dig-down requires the assignment family; for the plain non-multiexpr case
/// (the common one) the answer is simply "not a bare MULTIEXPR Param".
fn expr_is_multiexpr_param(expr: Option<&Expr>) -> bool {
    // We only positively detect a bare top-level Param MULTIEXPR (the case that
    // does not need the assignment-family dig-down). A FieldStore/SubsRef/
    // CoerceToDomain-wrapped MULTIEXPR would require the assignment family; if
    // one is ever encountered the targetlist render reaches processIndirection,
    // which panics precisely there.
    matches!(
        expr,
        Some(Expr::Param(p)) if p.paramkind == types_nodes::primnodes::ParamKind::PARAM_MULTIEXPR
    )
}

/// `count_nonjunk_tlist_entries(((Query*)sublink->subselect)->targetList)`
/// (ruleutils.c 7307) — the number of result columns of the multiassignment's
/// source SubLink.
fn multiexpr_remaining(sublink_node: Option<&Node<'_>>) -> PgResult<i32> {
    let sl = match sublink_node.and_then(|n| n.as_expr()) {
        Some(Expr::SubLink(s)) => s,
        _ => return Err(elog_error("multiassignment cur_ma_sublink is not a SubLink".into())),
    };
    let subq = sl.subselect.as_deref().ok_or_else(|| missing_field("MULTIEXPR sublink subselect"))?;
    let mut n = 0;
    for tle in subq.targetList.iter() {
        if !tle.resjunk {
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    /// A bare deparse context (no namespaces) with the given pretty flags.
    fn ctx<'mcx>(mcx: Mcx<'mcx>, pretty_flags: i32, wrap: i32) -> DeparseContext<'mcx> {
        DeparseContext {
            buf: types_stringinfo::StringInfo::new_in(mcx),
            namespaces: PgVec::new_in(mcx),
            resultDesc: None,
            targetList: PgVec::new_in(mcx),
            windowClause: PgVec::new_in(mcx),
            prettyFlags: pretty_flags,
            wrapColumn: wrap,
            indentLevel: 0,
            varprefix: false,
            colNamesVisible: true,
            inGroupBy: false,
            varInOrderBy: false,
            appendparents: None,
        }
    }

    fn bufstr(c: &DeparseContext<'_>) -> alloc::string::String {
        alloc::string::String::from_utf8(c.buf.data.as_slice().to_vec()).unwrap()
    }

    #[test]
    fn frame_options_rows_between() {
        // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        let cx = MemoryContext::new("frame");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0, -1);
        let fo = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_BETWEEN
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;
        get_window_frame_options(fo, None, None, &mut c).unwrap();
        // trailing space removed
        assert_eq!(bufstr(&c), "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    #[test]
    fn frame_options_range_unbounded_excl_ties() {
        let cx = MemoryContext::new("frame2");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0, -1);
        let fo = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_RANGE
            | FRAMEOPTION_BETWEEN
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_UNBOUNDED_FOLLOWING
            | FRAMEOPTION_EXCLUDE_TIES;
        get_window_frame_options(fo, None, None, &mut c).unwrap();
        assert_eq!(
            bufstr(&c),
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES"
        );
    }

    #[test]
    fn append_context_keyword_nonpretty_is_plain() {
        // With no PRETTYFLAG_INDENT, appendContextKeyword just appends the str.
        let cx = MemoryContext::new("ack");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0, -1);
        str_(&mut c, "SELECT 1   ").unwrap();
        append_context_keyword(&mut c, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1).unwrap();
        // non-pretty: trailing spaces NOT removed, keyword appended verbatim
        assert_eq!(bufstr(&c), "SELECT 1    WHERE ");
    }

    #[test]
    fn append_context_keyword_pretty_wraps_and_trims() {
        // With PRETTYFLAG_INDENT, trailing spaces are stripped and a newline +
        // indent is inserted before the keyword.
        let cx = MemoryContext::new("ack2");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, PRETTYFLAG_INDENT, 0);
        str_(&mut c, "SELECT 1   ").unwrap();
        // indentBefore=-STD takes indentLevel negative -> clamped to 0 for the
        // indent amount (Max(indentLevel,0)); indentPlus=1 -> one leading space.
        append_context_keyword(&mut c, "WHERE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1).unwrap();
        assert_eq!(bufstr(&c), "SELECT 1\n WHERE");
    }

    #[test]
    fn remove_trailing_spaces() {
        let cx = MemoryContext::new("rts");
        let mcx = cx.mcx();
        let mut c = ctx(mcx, 0, -1);
        str_(&mut c, "abc   ").unwrap();
        remove_string_info_spaces(&mut c);
        assert_eq!(bufstr(&c), "abc");
    }

    #[test]
    fn only_marker_inh() {
        let cx = MemoryContext::new("only");
        let mcx = cx.mcx();
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.inh = true;
        assert_eq!(only_marker(&rte), "");
        rte.inh = false;
        assert_eq!(only_marker(&rte), "ONLY ");
    }
}
