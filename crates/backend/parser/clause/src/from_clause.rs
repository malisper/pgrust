//! Port of `src/backend/parser/parse_clause.c`'s FROM-clause / JOIN subset
//! (PostgreSQL 18.3) — the F2 family layered on top of the F1 clause core.
//!
//! Ported 1:1 over the repo's split raw-[`Node`]/typed-[`Expr`] model with owned
//! `Vec`/`PgVec` lists and `Mcx<'mcx>`-threaded allocation:
//!
//!   * [`transformFromClause`] (parse_clause.c:112),
//!     [`setTargetTable`] (parse_clause.c:178),
//!     `transformFromClauseItem` (parse_clause.c:1053; the RangeVar /
//!     RangeSubselect / RangeFunction / RangeTableSample / JoinExpr arms),
//!     `transformTableEntry` (parse_clause.c:394),
//!     `getNSItemForSpecialRelationTypes` (parse_clause.c:1010),
//!     `transformRangeSubselect` (parse_clause.c:404),
//!     `transformRangeFunction` (parse_clause.c:462),
//!     `transformRangeTableSample` (parse_clause.c:685),
//!     and the JOIN machinery: `transformJoinUsingClause`,
//!     `transformJoinOnClause`, `extractRemainingColumns`,
//!     `buildVarFromNSColumn`, `buildMergedJoinVar`, `markRelsAsNulledBy`,
//!     `setNamespaceColumnVisibility`, `setNamespaceLateralState`.
//!
//! # Owned-tree divergences (representation only, no behavior change)
//!
//! The C out-params `(*top_nsitem, *namespace)` are carried as the returned
//! namespace `Vec`, whose LAST element is always the top nsitem (C:
//! `*namespace = lappend(my_namespace, nsitem); *top_nsitem = nsitem;`, and
//! `list_make1(nsitem)` in the leaf arms). `pstate.p_joinexprs` /
//! `pstate.p_nullingrels` are `PgVec` padded lazily exactly as the C `List *`
//! (an `rtindex == 0` `JoinExpr` / empty `Bitmapset` is the "not a join slot" /
//! "no nulling joins yet" placeholder). C's pointer-equality test
//! `u_colvar == (Node *) l_colvar` is carried as an explicit which-side marker
//! ([`MergedWhich`]) computed where C decides which node to return.
//!
//! `transformRangeTableSample` builds a `TableSampleClause`; the TABLESAMPLE
//! arm stores it in `RangeTblEntry.tablesample` as a `Node::TableSampleClause`
//! (the post-analysis carrier added for this).
//!
//! # Seams (panic-until-owner-lands)
//!
//! `parse_sub_analyze` (analyze — `backend-parser-analyze-seams`),
//! `FigureColname` (parse_target — `backend-parser-target-seams`),
//! `GetTsmRoutine` (tablesample registry — `backend-access-tablesample-core-seams`),
//! `LookupFuncName` / `get_func_rettype` (parse_func/lsyscache —
//! `backend-commands-functioncmds-seams`), plus the F1 seams.
//! `addRangeTableEntryForFunction` (parse_relation; panics until funcapi lands).
//!
//! # Deferred to follow-on families (NOT in this crate)
//!
//!   * F3a: on-conflict, `transformWindowDefinitions`.
//!   * F3b: `transformRangeTableFunc` (XMLTABLE), `transformJsonTable`.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::mcx::{alloc_in, Mcx, PgVec};

use ::types_core::{Index, InvalidOid, Oid, OidIsValid};
use ::types_error::{
    PgResult, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLESAMPLE_ARGUMENT, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use ::types_error::error::ERRCODE_DUPLICATE_ALIAS;
use ::utils_error::ereport;

use ::types_acl::acl::AclMode;
use ::types_storage::lock::{NoLock, RowExclusiveLock};

use ::types_tuple::heaptuple::{FLOAT8OID, INTERNALOID};

use ::types_core::primitive::AttrNumber;
use ::nodes::jointype::JoinType;
use ::nodes::nodes::{ntag, CmdType, Node, NodePtr};
use ::nodes::parsenodes::RTEKind;
use ::nodes::parsestmt::{
    ParseExprKind, ParseNamespaceColumn, ParseNamespaceItem, ParseState,
};
use ::nodes::nodesamplescan::TableSampleClause;
use ::nodes::primnodes::{
    AND_EXPR, CaseTestExpr, CoercionForm, Expr, JsonTablePathScan, JsonTableSiblingJoin, TableFunc,
    TFT_JSON_TABLE, TFT_XMLTABLE, Var, VarReturningType,
};
use ::nodes::rawexprnodes::{
    JsonFuncExpr, JsonOutput, JsonTable, JsonTableColumn, JsonTablePathSpec,
    JsonValueExpr as RawJsonValueExpr,
};
use ::nodes::primnodes::{
    JsonBehaviorType, JsonEncoding, JsonExprOp, JsonFormatType, JsonReturning, JsonTableColumnType,
    JsonWrapper, JsonQuotes,
};
use ::nodes::rawnodes::{
    A_Expr_Kind, Alias, JoinExpr, RangeFunction, RangeSubselect, RangeTableFunc,
    RangeTableSample, RangeTblRef, RangeVar, ResTarget,
};

use ::parsenodes::CoercionContext;

use ::nodes_core::makefuncs::{
    make_a_expr, make_const, make_func_call, make_json_format, make_relabel_type, make_var,
};
use ::nodes_core::nodefuncs::{expr_collation, expr_location, expr_type, expr_typmod};

use ::vars::var::contain_vars_of_level;
use ::parse_expr::transformExpr;
use ::parse_collate::{assign_expr_collations, assign_list_collations};
use ::coerce::{coerce_to_specific_type, coerce_type, select_common_type, select_common_typmod};
use ::parser_relation::{
    addNSItemToQuery, addRangeTableEntryForCTE, addRangeTableEntryForENR,
    addRangeTableEntryForFunction, addRangeTableEntryForJoin, addRangeTableEntryForRelation,
    addRangeTableEntryForSubquery, addRangeTableEntryForTableFunc, addRangeTableEntry,
    checkNameSpaceConflicts, isLockedRefname, markNullableIfNeeded, markVarForSelectPriv,
    parserOpenTable, scanNameSpaceForCTE, scanNameSpaceForENR,
};
use ::coerce::coerce_to_specific_type_typmod;
use ::parse_type::typenameTypeIdAndMod;
use ::table::table_close;

use parse_func_seams as parse_func;
use tablesample_core_seams as tsmapi;
use parser_analyze_seams as analyze;
use target_seams as parse_target;
use lsyscache_seams as lsyscache;

use ::samplescan::TsmRoutine;

use crate::{elog_error, errpos, str_val, transformWhereClause};

// ===========================================================================
// Local constants (parse_clause.c #includes / catalog OIDs).
// ===========================================================================

/// `COERCION_IMPLICIT` (parser/parse_coerce.h).
const COERCION_IMPLICIT: CoercionContext = CoercionContext::COERCION_IMPLICIT;
/// `COERCE_IMPLICIT_CAST` (nodes/primnodes.h).
const COERCE_IMPLICIT_CAST: CoercionForm = CoercionForm::COERCE_IMPLICIT_CAST;
/// `COERCE_EXPLICIT_CALL` (nodes/primnodes.h, first `CoercionForm` value).
const COERCE_EXPLICIT_CALL: CoercionForm = CoercionForm::COERCE_EXPLICIT_CALL;
/// `VAR_RETURNING_DEFAULT` (nodes/primnodes.h).
const VAR_RETURNING_DEFAULT: VarReturningType = VarReturningType::VAR_RETURNING_DEFAULT;

/// `TSM_HANDLEROID` (catalog/pg_type_d.h) — the OID of the `tsm_handler` type.
const TSM_HANDLEROID: Oid = 3310;

/// `JSONPATHOID` (catalog/pg_type_d.h) — the OID of the `jsonpath` type.
const JSONPATHOID: Oid = 4072;

/// `RELKIND_RELATION` (catalog/pg_class.h).
const RELKIND_RELATION: i8 = b'r' as i8;
/// `RELKIND_MATVIEW` (catalog/pg_class.h).
const RELKIND_MATVIEW: i8 = b'm' as i8;
/// `RELKIND_PARTITIONED_TABLE` (catalog/pg_class.h).
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

// ===========================================================================
// transformFromClause — parse_clause.c:112
// ===========================================================================

/// Process the FROM clause and add items to the query's range table,
/// joinlist, and namespace.
///
/// Note: we assume that the pstate's p_rtable, p_joinlist, and p_namespace
/// lists were initialized to NIL when the pstate was created. We will add
/// onto any entries already present --- this is needed for rule processing,
/// as well as for UPDATE and DELETE.
pub fn transformFromClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    frm_list: &[NodePtr<'mcx>],
) -> PgResult<()> {
    /*
     * The grammar will have produced a list of RangeVars, RangeSubselects,
     * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
     * entries to the rtable), check for duplicate refnames, and then add it
     * to the joinlist and namespace.
     *
     * Note we must process the items left-to-right for proper handling of
     * LATERAL references.
     */
    for fl in frm_list.iter() {
        let (n, mut namespace) = transformFromClauseItem(mcx, pstate, fl)?;

        checkNameSpaceConflicts(pstate, &pstate.p_namespace, &namespace)?;

        /* Mark the new namespace items as visible only to LATERAL */
        setNamespaceLateralState(&mut namespace, true, true);

        pstate.p_joinlist.try_reserve(1).map_err(|_| mcx.oom(0))?;
        pstate.p_joinlist.push(alloc_in(mcx, n)?);
        for nsitem in namespace.into_iter() {
            pstate.p_namespace.try_reserve(1).map_err(|_| mcx.oom(0))?;
            pstate.p_namespace.push(nsitem);
        }
    }

    /*
     * We're done parsing the FROM list, so make all namespace items
     * unconditionally visible.  Note that this will also reset lateral_only
     * for any namespace items that were already present when we were called;
     * but those should have been that way already.
     */
    setNamespaceLateralState(&mut pstate.p_namespace, false, true);

    Ok(())
}

// ===========================================================================
// setTargetTable — parse_clause.c:178
// ===========================================================================

/// Add the target relation of INSERT/UPDATE/DELETE/MERGE to the range table,
/// and make the special links to it in the ParseState.
///
/// Returns the rangetable index of the target relation.
pub fn setTargetTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    relation: &RangeVar<'mcx>,
    inh: bool,
    also_source: bool,
    required_perms: AclMode,
) -> PgResult<i32> {
    /*
     * ENRs hide tables of the same name, so we need to check for them first.
     * In contrast, CTEs don't hide tables (for this purpose).
     */
    if relation.schemaname.is_none()
        && scanNameSpaceForENR(pstate, relation.relname.as_deref().unwrap_or(""))
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "relation \"{}\" cannot be the target of a modifying statement",
                relation.relname.as_deref().unwrap_or("")
            ))
            .into_error());
    }

    /* Close old target; this could only happen for multi-action rules */
    if let Some(old) = pstate.p_target_relation.take() {
        table_close(old, NoLock)?;
    }

    /*
     * Open target rel and grab suitable lock (which we will hold till end of
     * transaction).
     */
    let rel = parserOpenTable(mcx, pstate, relation, RowExclusiveLock)?;
    pstate.p_target_relation = Some(rel);

    /*
     * Now build an RTE and a ParseNamespaceItem.  The RTE builder needs
     * `&pstate` (mut) and `&RelationData`; take the open `Relation` out of the
     * pstate during the call (it is `Drop` = table_close, so move it back
     * intact afterwards rather than borrow), and deref it to its
     * `RelationData`.
     */
    let rel = pstate
        .p_target_relation
        .take()
        .expect("setTargetTable: target relation just set");
    let result = addRangeTableEntryForRelation(
        mcx,
        pstate,
        &rel,
        RowExclusiveLock,
        copy_opt_alias(mcx, relation.alias.as_deref())?,
        inh,
        false,
    );
    pstate.p_target_relation = Some(rel);
    let mut nsitem = result?;

    /*
     * Override addRangeTableEntry's default ACL_SELECT permissions check, and
     * instead mark target table as requiring exactly the specified
     * permissions. The nsitem holds a snapshot of the perminfo; the live one
     * lives in p_rteperminfos at the RTE's perminfoindex.
     */
    let perminfoindex = nsitem
        .p_rte
        .as_deref()
        .map(|r| r.perminfoindex)
        .unwrap_or(0);
    if perminfoindex > 0 {
        pstate.p_rteperminfos[(perminfoindex - 1) as usize].requiredPerms = required_perms;
    }
    if let Some(pi) = nsitem.p_perminfo.as_deref_mut() {
        pi.requiredPerms = required_perms;
    }

    let rtindex = nsitem.p_rtindex;

    /* remember the RTE/nsitem as being the query target */
    pstate.p_target_nsitem = Some(alloc_in(mcx, clone_nsitem(mcx, &nsitem)?)?);

    /*
     * If UPDATE/DELETE, add table to joinlist and namespace.
     */
    if also_source {
        addNSItemToQuery(mcx, pstate, nsitem, true, true, true)?;
    }

    Ok(rtindex)
}

// ===========================================================================
// extractRemainingColumns — parse_clause.c:252
// ===========================================================================

/// Extract all not-in-common columns from column lists of a source table.
///
/// Returns the number of columns added, appending to the `res_*` vectors
/// (the caller-allocated `res_nscolumns[]` array is the tail of the Vec here).
#[allow(clippy::too_many_arguments)]
fn extractRemainingColumns<'mcx>(
    pstate: &ParseState<'mcx>,
    src_nscolumns: &[ParseNamespaceColumn],
    src_colnames: &[String],
    src_colnos: &mut Vec<i32>,
    res_colnames: &mut Vec<String>,
    res_colvars: &mut Vec<Node<'mcx>>,
    res_nscolumns: &mut Vec<ParseNamespaceColumn>,
) -> PgResult<usize> {
    /*
     * While we could just test "list_member_int(*src_colnos, attnum)" to
     * detect already-merged columns in the loop below, that would be O(N^2)
     * for a wide input table.  Instead build a bitmapset of just the merged
     * USING columns, which we won't add to within the main loop.
     */
    let prevcols: Vec<i32> = src_colnos.clone();

    let mut colcount = 0usize;
    let mut attnum = 0i32;
    for colname in src_colnames.iter() {
        attnum += 1;
        /* Non-dropped and not already merged? */
        if !colname.is_empty() && !prevcols.contains(&attnum) {
            /* Yes, so emit it as next output column */
            src_colnos.push(attnum);
            res_colnames.push(colname.clone());
            res_colvars.push(Node::mk_expr(
                *pstate.p_rtable.allocator(),
                Expr::Var(buildVarFromNSColumn(
                    pstate,
                    &src_nscolumns[(attnum - 1) as usize],
                )?),
            )?);
            /* Copy the input relation's nscolumn data for this column */
            res_nscolumns.push(src_nscolumns[(attnum - 1) as usize]);
            colcount += 1;
        }
    }
    Ok(colcount)
}

// ===========================================================================
// transformJoinUsingClause — parse_clause.c:306
// ===========================================================================

/// Build a complete ON clause from a partially-transformed USING list.
/// We are given lists of nodes representing left and right match columns.
/// Result is a transformed qualification expression.
fn transformJoinUsingClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    left_vars: &[Var],
    right_vars: &[Var],
) -> PgResult<Expr<'static>> {
    /*
     * We cheat a little bit here by building an untransformed operator tree
     * whose leaves are the already-transformed Vars.  This requires collusion
     * from transformExpr(), which normally could be expected to complain
     * about already-transformed subnodes.  However, this does mean that we
     * have to mark the columns as requiring SELECT privilege for ourselves;
     * transformExpr() won't do it.
     */
    let mut andargs: Vec<Node<'mcx>> = Vec::new();
    for (lvar, rvar) in left_vars.iter().zip(right_vars.iter()) {
        /* Require read access to the join variables */
        markVarForSelectPriv(mcx, pstate, lvar)?;
        markVarForSelectPriv(mcx, pstate, rvar)?;

        /* Now create the lvar = rvar join condition */
        let mut name = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 1)?;
        name.push(make_string_node(mcx, "=")?);
        let e = make_a_expr(
            A_Expr_Kind::AEXPR_OP,
            name,
            Some(alloc_in(mcx, Node::mk_var(mcx, lvar.clone())?)?),
            Some(alloc_in(mcx, Node::mk_var(mcx, rvar.clone())?)?),
            -1,
        );

        /* Prepare to combine into an AND clause, if multiple join columns */
        andargs.push(Node::mk_a_expr(mcx, e)?);
    }

    /* Only need an AND if there's more than one join column */
    let result: Node<'mcx> = if andargs.len() == 1 {
        andargs.pop().unwrap()
    } else {
        /*
         * makeBoolExpr(AND_EXPR, andargs, -1): the raw operator tree's args are
         * raw `A_Expr` nodes, so this is a raw `BoolExpr` carried as a `Node`.
         */
        let mut args = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, andargs.len())?;
        for a in andargs.into_iter() {
            args.push(alloc_in(mcx, a)?);
        }
        Node::mk_bool_expr(mcx, ::nodes::rawexprnodes::BoolExpr {
            boolop: AND_EXPR,
            args,
            location: -1,
        })?
    };

    /*
     * Since the references are already Vars, and are certainly from the input
     * relations, we don't have to go through the same pushups that
     * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
     * the operators, and we're done.
     */
    let result = transformExpr(pstate, Some(result), ParseExprKind::EXPR_KIND_JOIN_USING)?
        .ok_or_else(|| elog_error("transformJoinUsingClause: transformExpr returned NULL"))?;

    let result =
        ::coerce::coerce_to_boolean(mcx, Some(pstate), result, "JOIN/USING")?;

    Ok(result)
}

// ===========================================================================
// transformJoinOnClause — parse_clause.c:365
// ===========================================================================

/// Transform the qual conditions for JOIN/ON.
/// Result is a transformed qualification expression.
fn transformJoinOnClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    quals: Node<'mcx>,
    my_namespace: &mut [ParseNamespaceItem<'mcx>],
) -> PgResult<Expr<'static>> {
    /*
     * The namespace that the join expression should see is just the two
     * subtrees of the JOIN plus any outer references from upper pstate
     * levels.  Temporarily set this pstate's namespace accordingly.  (We need
     * not check for refname conflicts, because transformFromClauseItem()
     * already did.)  All namespace items are marked visible regardless of
     * LATERAL state.
     */
    setNamespaceLateralState(my_namespace, false, true);

    /* save_namespace = pstate->p_namespace; pstate->p_namespace = namespace; */
    let save_namespace = core::mem::replace(&mut pstate.p_namespace, clone_namespace(mcx, my_namespace)?);

    let result = transformWhereClause(
        mcx,
        pstate,
        Some(quals),
        ParseExprKind::EXPR_KIND_JOIN_ON,
        "JOIN/ON",
    );

    pstate.p_namespace = save_namespace;

    result.map(|r| r.expect("transformJoinOnClause: non-null quals must transform"))
}

// ===========================================================================
// transformTableEntry — parse_clause.c:394
// ===========================================================================

/// Transform a RangeVar (simple relation reference).
fn transformTableEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    r: &RangeVar<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    /* addRangeTableEntry does all the work */
    addRangeTableEntry(
        mcx,
        pstate,
        r,
        copy_opt_alias(mcx, r.alias.as_deref())?,
        r.inh,
        true,
    )
}

// ===========================================================================
// transformRangeSubselect — parse_clause.c:404
// ===========================================================================

/// Transform a sub-SELECT appearing in FROM.
fn transformRangeSubselect<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    r: &RangeSubselect<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    /*
     * Set p_expr_kind to show this parse level is recursing to a subselect.
     * We can't be nested within any expression, so don't need save-restore
     * logic here.
     */
    debug_assert!(pstate.p_expr_kind == ParseExprKind::EXPR_KIND_NONE);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_FROM_SUBSELECT;

    /*
     * If the subselect is LATERAL, make lateral_only names of this level
     * visible to it.
     */
    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = r.lateral;

    /*
     * Analyze and transform the subquery.  Note that if the subquery doesn't
     * have an alias, it can't be explicitly selected for locking, but locking
     * might still be required (if there is an all-tables locking clause).
     */
    let alias_name = r
        .alias
        .as_deref()
        .and_then(|a| a.aliasname.as_deref());
    let locked = isLockedRefname(pstate, alias_name);

    let subquery_node = r
        .subquery
        .as_deref()
        .ok_or_else(|| elog_error("transformRangeSubselect: subquery is NULL"))?;
    /* parentCTE = NULL for a FROM sub-SELECT */
    let query_node =
        analyze::parse_sub_analyze::call(mcx, subquery_node, pstate, None, locked, true)?;

    /* Restore state */
    pstate.p_lateral_active = false;
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_NONE;

    /*
     * Check that we got a SELECT.  Anything else should be impossible given
     * restrictions of the grammar, but check anyway.
     * (C: `!IsA(query, Query) || query->commandType != CMD_SELECT`.)
     */
    let query = match query_node.node_tag() {
        ntag::T_Query => query_node.expect_query().clone_in(mcx)?,
        _ => return Err(elog_error("unexpected non-SELECT command in subquery in FROM")),
    };
    if query.commandType != CmdType::CMD_SELECT {
        return Err(elog_error("unexpected non-SELECT command in subquery in FROM"));
    }

    /*
     * OK, build an RTE and nsitem for the subquery.
     */
    addRangeTableEntryForSubquery(
        mcx,
        pstate,
        query,
        copy_opt_alias(mcx, r.alias.as_deref())?,
        r.lateral,
        true,
    )
}

// ===========================================================================
// transformRangeFunction — parse_clause.c:462
// ===========================================================================

/// Transform a function call appearing in FROM.
fn transformRangeFunction<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    r: &RangeFunction<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let mut funcexprs: Vec<NodePtr<'mcx>> = Vec::new();
    let mut funcnames: Vec<String> = Vec::new();
    let mut coldeflists: Vec<PgVec<'mcx, NodePtr<'mcx>>> = Vec::new();

    /*
     * We make lateral_only names of this level visible, whether or not the
     * RangeFunction is explicitly marked LATERAL.  This is needed for SQL
     * spec compliance in the case of UNNEST(), and seems useful on
     * convenience grounds for all functions in FROM.
     */
    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;

    /*
     * Transform the raw expressions.
     */
    for lc in r.functions.iter() {
        /* Disassemble the function-call/column-def-list pairs */
        let pair = match lc.node_tag() {
            ntag::T_List => lc.expect_list(),
            _ => return Err(elog_error("transformRangeFunction: function item is not a List")),
        };
        debug_assert!(pair.len() == 2);
        let fexpr = &*pair[0];
        let coldeflist = match pair[1].node_tag() {
            ntag::T_List => copy_node_pgvec(mcx, pair[1].expect_list())?,
            // C: lsecond(pair) is a `List *` (possibly NIL == empty list).
            _ => PgVec::new_in(mcx),
        };
        let coldeflist_is_nil = coldeflist.is_empty();

        /*
         * If we find a function call unnest() with more than one argument and
         * no special decoration, transform it into separate unnest() calls on
         * each argument.
         */
        let mut handled_unnest = false;
        if let Some(fc) = fexpr.as_funccall() {
            if fc.funcname.len() == 1
                && str_val(&fc.funcname[0]) == Some("unnest")
                && fc.args.len() > 1
                && fc.agg_order.is_empty()
                && fc.agg_filter.is_none()
                && fc.over.is_none()
                && !fc.agg_star
                && !fc.agg_distinct
                && !fc.func_variadic
                && coldeflist_is_nil
            {
                for arg in fc.args.iter() {
                    let last_srf = clone_opt_node(mcx, pstate.p_last_srf.as_deref())?;

                    let newfc_name = system_func_name(mcx, "unnest")?;
                    let mut newfc_args = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 1)?;
                    newfc_args.push(alloc_in(mcx, (**arg).clone_in(mcx)?)?);
                    let newfc = make_func_call(
                        mcx,
                        newfc_name,
                        newfc_args,
                        COERCE_EXPLICIT_CALL,
                        fc.location,
                    )?;
                    let newfc_node = Node::mk_func_call(mcx, newfc)?;

                    let newfexpr = transformExpr(
                        pstate,
                        Some(newfc_node.clone_in(mcx)?),
                        ParseExprKind::EXPR_KIND_FROM_FUNCTION,
                    )?
                    .ok_or_else(|| elog_error("transformRangeFunction: transformExpr returned NULL"))?;
                    let newfexpr_node = Node::mk_expr(mcx, newfexpr.clone_in(mcx)?)?;

                    /* nodeFunctionscan.c requires SRFs to be at top level */
                    check_srf_top_level(pstate, last_srf.as_ref(), &newfexpr_node)?;

                    funcexprs.push(alloc_in(mcx, newfexpr_node)?);
                    funcnames.push(String::from(
                        parse_target::FigureColname::call(mcx, &newfc_node)?.as_str(),
                    ));
                    /* coldeflist is empty, so no error is possible */
                    coldeflists.push(PgVec::new_in(mcx));
                }
                handled_unnest = true;
            }
        }

        if handled_unnest {
            continue; /* done with this function item */
        }

        /* normal case ... */
        let last_srf = clone_opt_node(mcx, pstate.p_last_srf.as_deref())?;

        let newfexpr = transformExpr(
            pstate,
            Some(fexpr.clone_in(mcx)?),
            ParseExprKind::EXPR_KIND_FROM_FUNCTION,
        )?
        .ok_or_else(|| elog_error("transformRangeFunction: transformExpr returned NULL"))?;
        let newfexpr_node = Node::mk_expr(mcx, newfexpr.clone_in(mcx)?)?;

        /* nodeFunctionscan.c requires SRFs to be at top level */
        check_srf_top_level(pstate, last_srf.as_ref(), &newfexpr_node)?;

        funcexprs.push(alloc_in(mcx, newfexpr_node)?);
        funcnames.push(String::from(
            parse_target::FigureColname::call(mcx, fexpr)?.as_str(),
        ));

        if !coldeflist_is_nil && !r.coldeflist.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("multiple column definition lists are not allowed for the same function")
                .errposition(errpos(pstate, list_node_location(&r.coldeflist)?))
                .into_error());
        }

        coldeflists.push(coldeflist);
    }

    pstate.p_lateral_active = false;

    /*
     * We must assign collations now so that the RTE exposes correct collation
     * info for Vars created from it.
     */
    {
        let mut exprs = funcexprs_to_expr_vec(mcx, &funcexprs)?;
        assign_list_collations(Some(pstate), &mut exprs)?;
        store_back_funcexprs(mcx, &mut funcexprs, exprs)?;
    }

    /*
     * Install the top-level coldeflist if there was one (we already checked
     * that there was no conflicting per-function coldeflist).
     */
    if !r.coldeflist.is_empty() {
        if funcexprs.len() != 1 {
            let (msg, hint) = if r.is_rowsfrom {
                (
                    "ROWS FROM() with multiple functions cannot have a column definition list",
                    "Put a separate column definition list for each function inside ROWS FROM().",
                )
            } else {
                (
                    "UNNEST() with multiple arguments cannot have a column definition list",
                    "Use separate UNNEST() calls inside ROWS FROM(), and attach a column definition list to each one.",
                )
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(msg)
                .errhint(hint)
                .errposition(errpos(pstate, list_node_location(&r.coldeflist)?))
                .into_error());
        }
        if r.ordinality {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("WITH ORDINALITY cannot be used with a column definition list")
                .errhint("Put the column definition list inside ROWS FROM().")
                .errposition(errpos(pstate, list_node_location(&r.coldeflist)?))
                .into_error());
        }

        coldeflists = alloc::vec![copy_node_pgvec(mcx, &r.coldeflist)?];
    }

    /*
     * Mark the RTE as LATERAL if the user said LATERAL explicitly, or if
     * there are any lateral cross-references in it.
     */
    let funcexprs_list_node = funcexprs_as_list_node(mcx, &funcexprs)?;
    let is_lateral = r.lateral || contain_vars_of_level(&funcexprs_list_node, 0);

    /*
     * OK, build an RTE and nsitem for the function.
     */
    let funcnames_strs: Vec<::mcx::PgString<'mcx>> = {
        let mut v = Vec::with_capacity(funcnames.len());
        for n in funcnames.iter() {
            v.push(::mcx::PgString::from_str_in(n, mcx)?);
        }
        v
    };
    addRangeTableEntryForFunction(
        mcx,
        pstate,
        &funcnames_strs,
        &funcexprs,
        &coldeflists,
        r,
        is_lateral,
        true,
    )
}

// ===========================================================================
// transformRangeTableFunc — parse_clause.c:686
// ===========================================================================

/// Transform a raw `RangeTableFunc` (XMLTABLE) into a `TableFunc` RTE.
///
/// Transform the namespace clauses, the document-generating expression, the
/// row-generating expression, the column-generating expressions, and the
/// default value expressions. 1:1 port of `transformRangeTableFunc`
/// (parse_clause.c:686); currently only XMLTABLE (JSON_TABLE is
/// `transformJsonTable`, deferred).
fn transformRangeTableFunc<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rtf: &RangeTableFunc<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;
    use ::types_core::catalog::INT4OID;
    use ::types_error::error::ERRCODE_INVALID_TABLE_DEFINITION;
    use ::types_tuple::heaptuple::{TEXTOID, XMLOID};

    let mut tf = TableFunc::default();

    // Currently we only support XMLTABLE here.
    tf.functype = TFT_XMLTABLE;
    let construct_name = "XMLTABLE";
    let doc_type = XMLOID;

    // We make lateral_only names of this level visible, whether or not the
    // RangeTableFunc is explicitly marked LATERAL.
    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;

    // Transform and apply typecast to the row-generating expression ...
    let rowexpr_in = rtf
        .rowexpr
        .as_deref()
        .ok_or_else(|| elog_error("transformRangeTableFunc: rowexpr is NULL"))?;
    let row_t = transformExpr(
        pstate,
        Some(rowexpr_in.clone_in(mcx)?),
        ParseExprKind::EXPR_KIND_FROM_FUNCTION,
    )?
    .ok_or_else(|| elog_error("transformRangeTableFunc: rowexpr transformed to NULL"))?;
    // Bring the parser-arena `'static` coerce result into `mcx` so the in-place
    // collation assignment (which ties pstate and expr to one `'mcx`) and the
    // `TableFunc<'mcx>` store share the arena lifetime (invariant `Expr`).
    let mut rowexpr: Expr<'mcx> =
        coerce_to_specific_type(mcx, Some(pstate), row_t, TEXTOID, construct_name)?.clone_in(mcx)?;
    assign_expr_collations(Some(pstate), &mut rowexpr)?;
    tf.rowexpr = Some(alloc_in(mcx, rowexpr)?);

    // ... and to the document itself.
    let docexpr_in = rtf
        .docexpr
        .as_deref()
        .ok_or_else(|| elog_error("transformRangeTableFunc: docexpr is NULL"))?;
    let doc_t = transformExpr(
        pstate,
        Some(docexpr_in.clone_in(mcx)?),
        ParseExprKind::EXPR_KIND_FROM_FUNCTION,
    )?
    .ok_or_else(|| elog_error("transformRangeTableFunc: docexpr transformed to NULL"))?;
    let mut docexpr: Expr<'mcx> =
        coerce_to_specific_type(mcx, Some(pstate), doc_t, doc_type, construct_name)?.clone_in(mcx)?;
    assign_expr_collations(Some(pstate), &mut docexpr)?;
    tf.docexpr = Some(alloc_in(mcx, docexpr)?);

    // undef ordinality column number
    tf.ordinalitycol = -1;

    // Process column specs.
    let mut colnames: PgVec<'mcx, ::mcx::PgString<'mcx>> = PgVec::new_in(mcx);
    let mut coltypes: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut coltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut colcollations: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut colexprs: PgVec<'mcx, Option<::mcx::PgBox<'mcx, Expr>>> = PgVec::new_in(mcx);
    let mut coldefexprs: PgVec<'mcx, Option<::mcx::PgBox<'mcx, Expr>>> = PgVec::new_in(mcx);
    let mut notnulls: Option<::mcx::PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> = None;
    let mut names: Vec<String> = Vec::new();

    for (colno, col) in rtf.columns.iter().enumerate() {
        let rawc = col.expect_rangetablefunccol();
        let rawc_name = rawc
            .colname
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_default();

        colnames.push(::mcx::PgString::from_str_in(rawc_name.as_str(), mcx)?);

        // Determine the type/typmod. FOR ORDINALITY columns are INTEGER per
        // spec; the others are user-specified.
        let (typid, typmod): (Oid, i32);
        if rawc.for_ordinality {
            if tf.ordinalitycol != -1 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("only one FOR ORDINALITY column is allowed")
                    .errposition(errpos(pstate, rawc.location))
                    .into_error());
            }
            typid = INT4OID;
            typmod = -1;
            tf.ordinalitycol = colno as i32;
        } else {
            let type_name = rawc
                .typeName
                .as_deref()
                .ok_or_else(|| elog_error("transformRangeTableFunc: column typeName is NULL"))?;
            if type_name.setof {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg(format!("column \"{rawc_name}\" cannot be declared SETOF"))
                    .errposition(errpos(pstate, rawc.location))
                    .into_error());
            }
            let tn_pn = ::parse_type::raw_typename_to_parse(type_name)?;
            let (id, md) = typenameTypeIdAndMod(mcx, Some(&*pstate), &tn_pn)?;
            typid = id;
            typmod = md;
        }

        coltypes.push(typid);
        coltypmods.push(typmod);
        colcollations.push(lsyscache::get_typcollation::call(typid)?);

        // Transform the PATH and DEFAULT expressions.
        let colexpr: Option<::mcx::PgBox<'mcx, Expr<'mcx>>> = if let Some(ce) = rawc.colexpr.as_deref() {
            let t = transformExpr(
                pstate,
                Some(ce.clone_in(mcx)?),
                ParseExprKind::EXPR_KIND_FROM_FUNCTION,
            )?
            .ok_or_else(|| elog_error("transformRangeTableFunc: colexpr transformed to NULL"))?;
            // Bring the parser-arena `'static` coerce result into `mcx` (invariant
            // `Expr`) for in-place collation assignment and the `'mcx` store.
            let mut e: Expr<'mcx> =
                coerce_to_specific_type(mcx, Some(pstate), t, TEXTOID, construct_name)?.clone_in(mcx)?;
            assign_expr_collations(Some(pstate), &mut e)?;
            Some(alloc_in(mcx, e)?)
        } else {
            None
        };

        let coldefexpr: Option<::mcx::PgBox<'mcx, Expr<'mcx>>> = if let Some(cde) = rawc.coldefexpr.as_deref() {
            let t = transformExpr(
                pstate,
                Some(cde.clone_in(mcx)?),
                ParseExprKind::EXPR_KIND_FROM_FUNCTION,
            )?
            .ok_or_else(|| {
                elog_error("transformRangeTableFunc: coldefexpr transformed to NULL")
            })?;
            let mut e: Expr<'mcx> =
                coerce_to_specific_type_typmod(mcx, Some(pstate), t, typid, typmod, construct_name)?
                    .clone_in(mcx)?;
            assign_expr_collations(Some(pstate), &mut e)?;
            Some(alloc_in(mcx, e)?)
        } else {
            None
        };

        colexprs.push(colexpr);
        coldefexprs.push(coldefexpr);

        if rawc.is_not_null {
            notnulls = Some(::nodes_core::bitmapset::bms_add_member(
                mcx,
                notnulls.take(),
                colno as i32,
            )?);
        }

        // make sure column names are unique
        for j in &names {
            if j == &rawc_name {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("column name \"{rawc_name}\" is not unique"))
                    .errposition(errpos(pstate, rawc.location))
                    .into_error());
            }
        }
        names.push(rawc_name);
    }

    tf.colnames = Some(colnames);
    tf.coltypes = Some(coltypes);
    tf.coltypmods = Some(coltypmods);
    tf.colcollations = Some(colcollations);
    tf.colexprs = Some(colexprs);
    tf.coldefexprs = Some(coldefexprs);
    tf.notnulls = notnulls;

    // Namespaces, if any, also need to be transformed.
    if !rtf.namespaces.is_empty() {
        let mut ns_uris: PgVec<'mcx, ::mcx::PgBox<'mcx, Expr>> = PgVec::new_in(mcx);
        let mut ns_names: PgVec<'mcx, Option<::mcx::PgString<'mcx>>> = PgVec::new_in(mcx);
        let mut default_ns_seen = false;
        let mut seen_names: Vec<String> = Vec::new();

        for ns in rtf.namespaces.iter() {
            let r: &ResTarget<'mcx> = ns.expect_restarget();
            let r_val = r
                .val
                .as_deref()
                .ok_or_else(|| elog_error("transformRangeTableFunc: namespace val is NULL"))?;
            let t = transformExpr(
                pstate,
                Some(r_val.clone_in(mcx)?),
                ParseExprKind::EXPR_KIND_FROM_FUNCTION,
            )?
            .ok_or_else(|| {
                elog_error("transformRangeTableFunc: namespace uri transformed to NULL")
            })?;
            let mut ns_uri: Expr<'mcx> =
                coerce_to_specific_type(mcx, Some(pstate), t, TEXTOID, construct_name)?.clone_in(mcx)?;
            assign_expr_collations(Some(pstate), &mut ns_uri)?;
            ns_uris.push(alloc_in(mcx, ns_uri)?);

            // Verify consistency of name list: no dupes, only one DEFAULT.
            match r.name.as_deref() {
                Some(rn) => {
                    let rn_s = rn.to_string();
                    for existing in &seen_names {
                        if existing == &rn_s {
                            return Err(ereport(ERROR)
                                .errcode(ERRCODE_SYNTAX_ERROR)
                                .errmsg(format!("namespace name \"{rn_s}\" is not unique"))
                                .errposition(errpos(pstate, r.location))
                                .into_error());
                        }
                    }
                    seen_names.push(rn_s.clone());
                    ns_names.push(Some(::mcx::PgString::from_str_in(rn_s.as_str(), mcx)?));
                }
                None => {
                    if default_ns_seen {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg("only one default namespace is allowed")
                            .errposition(errpos(pstate, r.location))
                            .into_error());
                    }
                    default_ns_seen = true;
                    // We represent DEFAULT by a null pointer.
                    ns_names.push(None);
                }
            }
        }

        tf.ns_uris = Some(ns_uris);
        tf.ns_names = Some(ns_names);
    }

    tf.location = rtf.location;

    pstate.p_lateral_active = false;

    // Mark the RTE as LATERAL if the user said LATERAL explicitly, or if there
    // are any lateral cross-references in it.
    let tf_node = Node::mk_table_func(mcx, tf)?;
    let is_lateral = rtf.lateral || contain_vars_of_level(&tf_node, 0);
    let tf_back = tf_node
        .into_tablefunc()
        .ok_or_else(|| elog_error("transformRangeTableFunc: not a TableFunc node"))?;

    let alias = copy_opt_alias(mcx, rtf.alias.as_deref())?;
    addRangeTableEntryForTableFunc(mcx, pstate, tf_back, alias, is_lateral, true)
}

// ===========================================================================
// transformJsonTable — parse_jsontable.c (JSON_TABLE())
// ===========================================================================

/// `JsonTableParseContext` (parse_jsontable.c) — context threaded through the
/// JSON_TABLE column-transformation helpers. `pstate`/`jt`/`tf` are passed
/// explicitly (not embedded) so the borrow checker can keep `tf`/the column
/// vectors mutable while `jt` is only read; `pathNames`/`pathNameId` carry the
/// shared name-uniqueness state.
struct JsonTableParseContext {
    /// `int pathNameId` — path-name id counter.
    pathNameId: i32,
    /// `List *pathNames` — list of all path and column names.
    pathNames: Vec<String>,
}

/// `isCompositeType(typid)` (parse_jsontable.c) — true for the types JSON_TABLE
/// handles with `JSON_QUERY()` rather than `JSON_VALUE()`.
fn isCompositeType(typid: Oid) -> PgResult<bool> {
    use ::parsenodes::{TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN};
    use ::types_tuple::heaptuple::{JSONBOID, JSONOID};
    use ::types_tuple::heaptuple::RECORDOID;

    let typtype = lsyscache::get_typtype::call(typid)? as i8;
    let type_is_array = lsyscache::get_element_type::call(typid)?.is_some();

    Ok(typid == JSONOID
        || typid == JSONBOID
        || typid == RECORDOID
        || type_is_array
        || typtype == TYPTYPE_COMPOSITE
        // domain over one of the above?
        || (typtype == TYPTYPE_DOMAIN
            && isCompositeType(lsyscache::get_base_type::call(typid)?)?))
}

/// `LookupPathOrColumnName(cxt, name)` (parse_jsontable.c) — true if `name` is
/// already present in the shared name list.
fn LookupPathOrColumnName(cxt: &JsonTableParseContext, name: &str) -> bool {
    cxt.pathNames.iter().any(|n| n == name)
}

/// `generateJsonTablePathName(cxt)` (parse_jsontable.c) — generate a fresh unique
/// JSON_TABLE path name and record it in the shared list.
fn generateJsonTablePathName(cxt: &mut JsonTableParseContext) -> String {
    let name = format!("json_table_path_{}", cxt.pathNameId);
    cxt.pathNameId += 1;
    cxt.pathNames.push(name.clone());
    name
}

/// `CheckDuplicateColumnOrPathNames(cxt, columns)` (parse_jsontable.c) — recurse
/// over the column definitions checking that no column / path name is duplicated.
fn CheckDuplicateColumnOrPathNames<'mcx>(
    pstate: &ParseState<'mcx>,
    cxt: &mut JsonTableParseContext,
    columns: &[NodePtr<'mcx>],
) -> PgResult<()> {
    for col in columns.iter() {
        let jtc: &JsonTableColumn<'mcx> = col.expect_jsontablecolumn();

        if jtc.coltype == JsonTableColumnType::JTC_NESTED {
            let pathspec = jtc
                .pathspec
                .as_deref()
                .ok_or_else(|| elog_error("CheckDuplicateColumnOrPathNames: NESTED without pathspec"))?;
            if let Some(name) = pathspec.name.as_deref() {
                let name_s = name.to_string();
                if LookupPathOrColumnName(cxt, &name_s) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_ALIAS)
                        .errmsg(format!(
                            "duplicate JSON_TABLE column or path name: {name_s}"
                        ))
                        .errposition(errpos(pstate, pathspec.name_location))
                        .into_error());
                }
                cxt.pathNames.push(name_s);
            }

            CheckDuplicateColumnOrPathNames(pstate, cxt, &jtc.columns)?;
        } else {
            let name = jtc
                .name
                .as_deref()
                .ok_or_else(|| elog_error("CheckDuplicateColumnOrPathNames: column without name"))?
                .to_string();
            if LookupPathOrColumnName(cxt, &name) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_ALIAS)
                    .errmsg(format!("duplicate JSON_TABLE column or path name: {name}"))
                    .errposition(errpos(pstate, jtc.location))
                    .into_error());
            }
            cxt.pathNames.push(name);
        }
    }
    Ok(())
}

/// `makeJsonTablePathScan(pathspec, errorOnError, colMin, colMax, childplan)`
/// (parse_jsontable.c) — build a `JsonTablePathScan` plan node carrying the
/// jsonpath `Const` value, the path name, and the covered-column range. The
/// trivial C `JsonTablePath` wrapper is collapsed into the scan's `path`/`name`.
fn makeJsonTablePathScan<'mcx>(
    mcx: Mcx<'mcx>,
    pathspec: &JsonTablePathSpec<'mcx>,
    errorOnError: bool,
    colMin: i32,
    colMax: i32,
    childplan: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    use ::types_tuple::Datum;

    // Assert(IsA(pathspec->string, A_Const));
    // pathstring = castNode(A_Const, pathspec->string)->val.sval.sval;
    let string_node = pathspec
        .string
        .as_deref()
        .ok_or_else(|| elog_error("makeJsonTablePathScan: pathspec->string is NULL"))?;
    let a_const = string_node.expect_a_const();
    let val = a_const
        .val
        .as_deref()
        .ok_or_else(|| elog_error("makeJsonTablePathScan: A_Const has no value"))?;
    let pathstring = str_val(val)
        .ok_or_else(|| elog_error("makeJsonTablePathScan: A_Const value is not a String"))?;

    // value = makeConst(JSONPATHOID, -1, InvalidOid, -1,
    //                   DirectFunctionCall1(jsonpath_in, CStringGetDatum(pathstring)),
    //                   false, false);
    // jsonpath_in returns the flattened on-disk jsonpath image, which already
    // carries its own 4-byte varlena length header (SET_VARSIZE) — pass it
    // through as the by-reference Const value verbatim (no re-wrapping).
    let image = adt_jsonpath::jsonpath_in(mcx, pathstring.as_bytes(), None)?
        .ok_or_else(|| elog_error("makeJsonTablePathScan: jsonpath_in returned NULL"))?;
    let value_datum: Datum<'mcx> = Datum::ByRef(image);
    let value = make_const(
        mcx,
        JSONPATHOID,
        -1,
        InvalidOid,
        -1,
        value_datum,
        false,
        false,
    )?;
    let value_node = Node::mk_const(mcx, value)?;

    let name = match pathspec.name.as_deref() {
        Some(n) => Some(::mcx::PgString::from_str_in(&n.to_string(), mcx)?),
        None => None,
    };

    let child = match childplan {
        Some(c) => Some(alloc_in(mcx, c)?),
        None => None,
    };

    Node::mk_json_table_path_scan(
        mcx,
        JsonTablePathScan {
            path: alloc_in(mcx, value_node)?,
            name,
            errorOnError,
            child,
            colMin,
            colMax,
        },
    )
}

/// `makeJsonTableSiblingJoin(lplan, rplan)` (parse_jsontable.c) — build a
/// `JsonTableSiblingJoin` plan node that UNIONs the rows of its two children.
fn makeJsonTableSiblingJoin<'mcx>(
    mcx: Mcx<'mcx>,
    lplan: Node<'mcx>,
    rplan: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk_json_table_sibling_join(
        mcx,
        JsonTableSiblingJoin {
            lplan: alloc_in(mcx, lplan)?,
            rplan: alloc_in(mcx, rplan)?,
        },
    )
}

/// `transformJsonTableColumn(jtc, contextItemExpr, passingArgs)`
/// (parse_jsontable.c) — turn a JSON_TABLE column definition into a raw
/// `JsonFuncExpr` (`JSON_VALUE`/`JSON_QUERY`/`JSON_EXISTS`). The `coltype` passed
/// here already reflects the JTC_REGULAR→JTC_FORMATTED promotion the caller did.
fn transformJsonTableColumn<'mcx>(
    mcx: Mcx<'mcx>,
    coltype: JsonTableColumnType,
    jtc: &JsonTableColumn<'mcx>,
    context_item_expr: Node<'mcx>,
    passing_args: &[NodePtr<'mcx>],
) -> PgResult<JsonFuncExpr<'mcx>> {
    let op = if coltype == JsonTableColumnType::JTC_REGULAR {
        JsonExprOp::JSON_VALUE_OP
    } else if coltype == JsonTableColumnType::JTC_EXISTS {
        JsonExprOp::JSON_EXISTS_OP
    } else {
        JsonExprOp::JSON_QUERY_OP
    };

    // Pass the column name so any runtime JsonExpr errors can print it.
    let jtc_name = jtc
        .name
        .as_deref()
        .ok_or_else(|| elog_error("transformJsonTableColumn: column without name"))?
        .to_string();
    let column_name = ::mcx::PgString::from_str_in(&jtc_name, mcx)?;

    // context_item = makeJsonValueExpr((Expr *) contextItemExpr, NULL,
    //                                  makeJsonFormat(JS_FORMAT_DEFAULT,
    //                                                 JS_ENC_DEFAULT, -1));
    // The raw JsonFuncExpr.context_item carries the RAW JsonValueExpr (a Node*
    // raw_expr), so build it directly rather than via make_json_value_expr (which
    // yields the cooked, Box<Expr> form).
    let context_item = RawJsonValueExpr {
        raw_expr: Some(alloc_in(mcx, context_item_expr)?),
        formatted_expr: None,
        format: Some(make_json_format(
            JsonFormatType::JS_FORMAT_DEFAULT,
            JsonEncoding::JS_ENC_DEFAULT,
            -1,
        )),
    };

    // pathspec
    let pathspec: NodePtr<'mcx> = if let Some(ps) = jtc.pathspec.as_deref() {
        let string = ps
            .string
            .as_deref()
            .ok_or_else(|| elog_error("transformJsonTableColumn: pathspec->string is NULL"))?;
        alloc_in(mcx, string.clone_in(mcx)?)?
    } else {
        // Construct default path as '$."column_name"'.
        let mut path: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        path.try_reserve(2).map_err(|_| mcx.oom(0))?;
        path.push(b'$');
        path.push(b'.');
        adt_json::escape_json(&mut path, jtc_name.as_bytes())?;
        let path_str = String::from_utf8_lossy(path.as_slice()).into_owned();
        make_string_const(mcx, &path_str, -1)?
    };

    // output = makeNode(JsonOutput); output->typeName = jtc->typeName;
    //          output->returning = makeNode(JsonReturning);
    //          output->returning->format = jtc->format;
    let returning = JsonReturning {
        format: jtc.format,
        typid: InvalidOid,
        typmod: 0,
    };
    let output = JsonOutput {
        type_name: match jtc.type_name.as_deref() {
            Some(t) => Some(alloc_in(mcx, t.clone_in(mcx)?)?),
            None => None,
        },
        returning: Some(returning),
    };

    let mut passing: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    passing
        .try_reserve(passing_args.len())
        .map_err(|_| mcx.oom(0))?;
    for a in passing_args.iter() {
        passing.push(alloc_in(mcx, a.clone_in(mcx)?)?);
    }

    Ok(JsonFuncExpr {
        op,
        column_name: Some(column_name),
        context_item: Some(alloc_in(mcx, context_item)?),
        pathspec: Some(pathspec),
        passing,
        output: Some(alloc_in(mcx, output)?),
        on_empty: match jtc.on_empty.as_deref() {
            Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        },
        on_error: match jtc.on_error.as_deref() {
            Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        },
        wrapper: jtc.wrapper,
        quotes: jtc.quotes,
        location: jtc.location,
    })
}

/// `makeStringConst(str, location)` (gram.y / makefuncs.c) — build an `A_Const`
/// string literal `Node`, returned as a raw `Node`.
fn make_string_const<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    location: i32,
) -> PgResult<NodePtr<'mcx>> {
    let str_node = Node::mk_string(
        mcx,
        ::nodes::value::StringNode {
            sval: ::mcx::PgString::from_str_in(s, mcx)?,
        },
    )?;
    let a_const = ::nodes::rawnodes::A_Const {
        val: Some(alloc_in(mcx, str_node)?),
        isnull: false,
        location,
    };
    alloc_in(mcx, Node::mk_a_const(mcx, a_const)?)
}

/// `transformJsonTableColumns(cxt, columns, passingArgs, pathspec)`
/// (parse_jsontable.c) — transform the (non-nested) columns of one scope, append
/// their `JsonExpr` nodes to `tf`, recurse into nested columns, and return the
/// `JsonTablePathScan` plan that supplies the source row.
fn transformJsonTableColumns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cxt: &mut JsonTableParseContext,
    jt: &JsonTable<'mcx>,
    tf: &mut TableFunc<'mcx>,
    columns: &[NodePtr<'mcx>],
    passing_args: &[NodePtr<'mcx>],
    pathspec: &JsonTablePathSpec<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut ordinality_found = false;
    let error_on_error = match jt.on_error.as_deref() {
        Some(b) => b.expect_jsonbehavior().btype == JsonBehaviorType::JSON_BEHAVIOR_ERROR,
        None => false,
    };

    // contextItemTypid = exprType(tf->docexpr);
    let context_item_typid = {
        let docexpr = tf
            .docexpr
            .as_deref()
            .ok_or_else(|| elog_error("transformJsonTableColumns: tf->docexpr is NULL"))?;
        expr_type(Some(docexpr))?
    };

    // Start of column range.
    let col_min = tf.colvalexprs.as_ref().map_or(0, |v| v.len()) as i32;

    for col in columns.iter() {
        let rawc: &JsonTableColumn<'mcx> = col.expect_jsontablecolumn();

        // The effective coltype: JTC_REGULAR may be promoted to JTC_FORMATTED.
        let mut eff_coltype = rawc.coltype;

        if rawc.coltype != JsonTableColumnType::JTC_NESTED {
            let name = rawc
                .name
                .as_deref()
                .ok_or_else(|| elog_error("transformJsonTableColumns: column without name"))?
                .to_string();
            let colnames = tf
                .colnames
                .get_or_insert_with(|| PgVec::new_in(mcx));
            colnames.try_reserve(1).map_err(|_| mcx.oom(0))?;
            colnames.push(::mcx::PgString::from_str_in(&name, mcx)?);
        }

        let typid: Oid;
        let typmod: i32;
        let mut typcoll: Oid = InvalidOid;
        let colexpr: Option<Expr<'mcx>>;

        match rawc.coltype {
            JsonTableColumnType::JTC_FOR_ORDINALITY => {
                if ordinality_found {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg("only one FOR ORDINALITY column is allowed")
                        .errposition(errpos(pstate, rawc.location))
                        .into_error());
                }
                ordinality_found = true;
                colexpr = None;
                typid = ::types_core::catalog::INT4OID;
                typmod = -1;
            }
            JsonTableColumnType::JTC_REGULAR
            | JsonTableColumnType::JTC_FORMATTED
            | JsonTableColumnType::JTC_EXISTS => {
                if rawc.coltype == JsonTableColumnType::JTC_REGULAR {
                    let (id, md) = {
                        let tn = rawc.type_name.as_deref().ok_or_else(|| {
                            elog_error("transformJsonTableColumns: JTC_REGULAR without typeName")
                        })?;
                        let tn_pn = ::parse_type::raw_typename_to_parse(tn)?;
                        typenameTypeIdAndMod(mcx, Some(&*pstate), &tn_pn)?
                    };
                    // Promote to JTC_FORMATTED if the type is better handled by
                    // JSON_QUERY() or non-default WRAPPER/QUOTES is specified.
                    if isCompositeType(id)?
                        || rawc.quotes != JsonQuotes::JS_QUOTES_UNSPEC
                        || rawc.wrapper != JsonWrapper::JSW_UNSPEC
                    {
                        eff_coltype = JsonTableColumnType::JTC_FORMATTED;
                    }
                    let _ = (id, md);
                }

                // param = makeNode(CaseTestExpr) with the context item type.
                let param = Expr::CaseTestExpr(CaseTestExpr {
                    collation: InvalidOid,
                    typeId: context_item_typid,
                    typeMod: -1,
                });
                let param_node = Node::mk_expr(mcx, param)?;

                let jfe = transformJsonTableColumn(
                    mcx,
                    eff_coltype,
                    rawc,
                    param_node,
                    passing_args,
                )?;
                let jfe_node = Node::mk_json_func_expr(mcx, jfe)?;

                // Bring the parser-arena `'static` transform result into `mcx`
                // (invariant `Expr`) for in-place collation assignment and the
                // `'mcx` `colvalexprs` store below.
                let mut ce: Expr<'mcx> = transformExpr(
                    pstate,
                    Some(jfe_node),
                    ParseExprKind::EXPR_KIND_FROM_FUNCTION,
                )?
                .ok_or_else(|| {
                    elog_error("transformJsonTableColumns: column transformed to NULL")
                })?
                .clone_in(mcx)?;
                assign_expr_collations(Some(pstate), &mut ce)?;

                typid = expr_type(Some(&ce))?;
                typmod = expr_typmod(Some(&ce))?;
                typcoll = expr_collation(Some(&ce))?;
                colexpr = Some(ce);
            }
            JsonTableColumnType::JTC_NESTED => {
                continue;
            }
        }

        let coltypes = tf.coltypes.get_or_insert_with(|| PgVec::new_in(mcx));
        coltypes.try_reserve(1).map_err(|_| mcx.oom(0))?;
        coltypes.push(typid);
        let coltypmods = tf.coltypmods.get_or_insert_with(|| PgVec::new_in(mcx));
        coltypmods.try_reserve(1).map_err(|_| mcx.oom(0))?;
        coltypmods.push(typmod);
        let colcollations = tf.colcollations.get_or_insert_with(|| PgVec::new_in(mcx));
        colcollations.try_reserve(1).map_err(|_| mcx.oom(0))?;
        colcollations.push(typcoll);
        let colvalexprs = tf.colvalexprs.get_or_insert_with(|| PgVec::new_in(mcx));
        colvalexprs.try_reserve(1).map_err(|_| mcx.oom(0))?;
        colvalexprs.push(match colexpr {
            Some(e) => Some(alloc_in(mcx, e)?),
            None => None,
        });
    }

    // End of column range.
    let cur_len = tf.colvalexprs.as_ref().map_or(0, |v| v.len()) as i32;
    let (col_min, col_max) = if cur_len == col_min {
        // No columns in this Scan beside the nested ones.
        (-1, -1)
    } else {
        (col_min, cur_len - 1)
    };

    // Recursively transform nested columns.
    let childplan =
        transformJsonTableNestedColumns(mcx, pstate, cxt, jt, tf, passing_args, columns)?;

    // Create a "parent" scan responsible for all columns handled above.
    makeJsonTablePathScan(mcx, pathspec, error_on_error, col_min, col_max, childplan)
}

/// `transformJsonTableNestedColumns(cxt, passingArgs, columns)`
/// (parse_jsontable.c) — recurse into the NESTED COLUMNS clauses, combining their
/// plans with sibling joins (a UNION of their row sets).
fn transformJsonTableNestedColumns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cxt: &mut JsonTableParseContext,
    jt: &JsonTable<'mcx>,
    tf: &mut TableFunc<'mcx>,
    passing_args: &[NodePtr<'mcx>],
    columns: &[NodePtr<'mcx>],
) -> PgResult<Option<Node<'mcx>>> {
    let mut plan: Option<Node<'mcx>> = None;

    for col in columns.iter() {
        let jtc: &JsonTableColumn<'mcx> = col.expect_jsontablecolumn();

        if jtc.coltype != JsonTableColumnType::JTC_NESTED {
            continue;
        }

        // jtc->pathspec->name == NULL → generate one.
        // `jtc` is borrowed from the (immutable) `columns` slice, so the C
        // in-place mutation of `pathspec->name` is reproduced by cloning the
        // pathspec and filling in a generated name when absent (the cloned spec
        // is what the scan stores; the generated name is also recorded in the
        // shared list by CheckDuplicateColumnOrPathNames-time generation).
        let pathspec_src = jtc
            .pathspec
            .as_deref()
            .ok_or_else(|| elog_error("transformJsonTableNestedColumns: NESTED without pathspec"))?;
        let mut pathspec = pathspec_src.clone_in(mcx)?;
        if pathspec.name.is_none() {
            let gen = generateJsonTablePathName(cxt);
            pathspec.name = Some(::mcx::PgString::from_str_in(&gen, mcx)?);
        }

        let nested = transformJsonTableColumns(
            mcx,
            pstate,
            cxt,
            jt,
            tf,
            &jtc.columns,
            passing_args,
            &pathspec,
        )?;

        plan = Some(match plan {
            Some(p) => makeJsonTableSiblingJoin(mcx, p, nested)?,
            None => nested,
        });
    }

    Ok(plan)
}

/// Transform a raw `JsonTable` into a `TableFunc` and add it as a range-table
/// entry. 1:1 port of `transformJsonTable` (parse_jsontable.c).
fn transformJsonTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    jt: &JsonTable<'mcx>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    let root_path_src = jt
        .pathspec
        .as_deref()
        .ok_or_else(|| elog_error("transformJsonTable: jt->pathspec is NULL"))?;

    // if (jt->on_error && btype not ERROR/EMPTY/EMPTY_ARRAY) ereport.
    if let Some(oe) = jt.on_error.as_deref() {
        let b = oe.expect_jsonbehavior();
        if b.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR
            && b.btype != JsonBehaviorType::JSON_BEHAVIOR_EMPTY
            && b.btype != JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid {} behavior", "ON ERROR"))
                .errdetail(
                    "Only EMPTY [ ARRAY ] or ERROR is allowed in the top-level ON ERROR clause.",
                )
                .errposition(errpos(pstate, b.location))
                .into_error());
        }
    }

    let mut cxt = JsonTableParseContext {
        pathNameId: 0,
        pathNames: Vec::new(),
    };

    // The root pathspec's name (generate one if absent). The owned model takes a
    // working copy of the root pathspec so it can fill in a generated name (the C
    // mutates rootPathSpec->name in place).
    let mut root_path = root_path_src.clone_in(mcx)?;
    if root_path.name.is_none() {
        let gen = generateJsonTablePathName(&mut cxt);
        root_path.name = Some(::mcx::PgString::from_str_in(&gen, mcx)?);
    } else {
        // cxt.pathNames = list_make1(rootPathSpec->name)
        cxt.pathNames
            .push(root_path.name.as_deref().unwrap().to_string());
    }
    CheckDuplicateColumnOrPathNames(pstate, &mut cxt, &jt.columns)?;

    // Make lateral_only names of this level visible.
    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;

    let mut tf = TableFunc::default();
    tf.functype = TFT_JSON_TABLE;

    // Build the dummy JSON_TABLE_OP JsonFuncExpr and transform it into docexpr.
    let jfe = JsonFuncExpr {
        op: JsonExprOp::JSON_TABLE_OP,
        column_name: None,
        context_item: match jt.context_item.as_deref() {
            Some(c) => Some(alloc_in(mcx, c.clone_in(mcx)?)?),
            None => None,
        },
        // jfe->pathspec = (Node *) rootPathSpec->string;
        pathspec: match root_path.string.as_deref() {
            Some(s) => Some(alloc_in(mcx, s.clone_in(mcx)?)?),
            None => None,
        },
        passing: {
            let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            v.try_reserve(jt.passing.len()).map_err(|_| mcx.oom(0))?;
            for a in jt.passing.iter() {
                v.push(alloc_in(mcx, a.clone_in(mcx)?)?);
            }
            v
        },
        output: None,
        on_empty: None,
        on_error: match jt.on_error.as_deref() {
            Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        },
        wrapper: JsonWrapper::JSW_UNSPEC,
        quotes: JsonQuotes::JS_QUOTES_UNSPEC,
        location: jt.location,
    };
    let jfe_node = Node::mk_json_func_expr(mcx, jfe)?;
    let docexpr = transformExpr(
        pstate,
        Some(jfe_node),
        ParseExprKind::EXPR_KIND_FROM_FUNCTION,
    )?
    .ok_or_else(|| elog_error("transformJsonTable: docexpr transformed to NULL"))?;
    tf.docexpr = Some(alloc_in(mcx, docexpr.clone_in(mcx)?)?);

    // Create the row-pattern plan (also fills tf->colvalexprs etc.).
    let plan = transformJsonTableColumns(
        mcx,
        pstate,
        &mut cxt,
        jt,
        &mut tf,
        &jt.columns,
        &jt.passing,
        &root_path,
    )?;
    tf.plan = Some(alloc_in(mcx, plan)?);

    // tf->passingvalexprs = copyObject(((JsonExpr *) tf->docexpr)->passing_values);
    {
        let je = tf
            .docexpr
            .as_deref()
            .ok_or_else(|| elog_error("transformJsonTable: tf->docexpr is NULL"))?
            .as_jsonexpr()
            .ok_or_else(|| elog_error("transformJsonTable: tf->docexpr is not a JsonExpr"))?;
        let mut pv: PgVec<'mcx, ::mcx::PgBox<'mcx, Expr>> = PgVec::new_in(mcx);
        pv.try_reserve(je.passing_values.len())
            .map_err(|_| mcx.oom(0))?;
        for e in je.passing_values.iter() {
            pv.push(alloc_in(mcx, e.clone_in(mcx)?)?);
        }
        tf.passingvalexprs = Some(pv);
    }

    tf.ordinalitycol = -1;
    tf.location = jt.location;

    pstate.p_lateral_active = false;

    // Mark LATERAL if requested or if there are lateral cross-references.
    let tf_node = Node::mk_table_func(mcx, tf)?;
    let is_lateral = jt.lateral || contain_vars_of_level(&tf_node, 0);
    let tf_back = tf_node
        .into_tablefunc()
        .ok_or_else(|| elog_error("transformJsonTable: not a TableFunc node"))?;

    let alias = copy_opt_alias(mcx, jt.alias.as_deref())?;
    addRangeTableEntryForTableFunc(mcx, pstate, tf_back, alias, is_lateral, true)
}

// ===========================================================================
// transformRangeTableSample — parse_clause.c:685
// ===========================================================================

/// Transform a TABLESAMPLE clause.
///
/// Caller has already transformed rts->relation, we just have to validate
/// the remaining fields and create a TableSampleClause node.
fn transformRangeTableSample<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rts: &RangeTableSample<'mcx>,
) -> PgResult<TableSampleClause<'mcx>> {
    /*
     * To validate the sample method name, look up the handler function, which
     * has the same name, one dummy INTERNAL argument, and a result type of
     * tsm_handler.
     */
    let method_names: Vec<::mcx::PgString<'mcx>> = {
        let mut v = Vec::with_capacity(rts.method.len());
        for n in rts.method.iter() {
            v.push(::mcx::PgString::from_str_in(str_val(n).unwrap_or(""), mcx)?);
        }
        v
    };
    let funcargtypes: [Oid; 1] = [INTERNALOID];

    let handler_oid =
        parse_func::lookup_func_name::call(&method_names, 1, &funcargtypes, true)?;

    /* we want error to complain about no-such-method, not no-such-function */
    if !OidIsValid(handler_oid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "tablesample method {} does not exist",
                name_list_to_string(&rts.method)
            ))
            .errposition(errpos(pstate, rts.location))
            .into_error());
    }

    /* check that handler has correct return type */
    if lsyscache::get_func_rettype::call(handler_oid)? != TSM_HANDLEROID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "function {} must return type {}",
                name_list_to_string(&rts.method),
                "tsm_handler"
            ))
            .errposition(errpos(pstate, rts.location))
            .into_error());
    }

    /* OK, run the handler to get TsmRoutine, for argument type info */
    let tsm: ::mcx::PgBox<'mcx, TsmRoutine> = tsmapi::get_tsm_routine_oid::call(mcx, handler_oid)?;

    let mut tablesample = TableSampleClause {
        tsmhandler: handler_oid,
        ..TableSampleClause::default()
    };

    /* check user provided the expected number of arguments */
    if rts.args.len() != tsm.parameterTypes.len() {
        let n = tsm.parameterTypes.len();
        let msg = if n == 1 {
            format!(
                "tablesample method {} requires {} argument, not {}",
                name_list_to_string(&rts.method),
                n,
                rts.args.len()
            )
        } else {
            format!(
                "tablesample method {} requires {} arguments, not {}",
                name_list_to_string(&rts.method),
                n,
                rts.args.len()
            )
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT)
            .errmsg(msg)
            .errposition(errpos(pstate, rts.location))
            .into_error());
    }

    /*
     * Transform the arguments, typecasting them as needed.  Note we must also
     * assign collations now, because assign_query_collations() doesn't
     * examine any substructure of RTEs.
     */
    let mut fargs: PgVec<'mcx, Expr<'mcx>> = PgVec::new_in(mcx);
    for (larg, &argtype) in rts.args.iter().zip(tsm.parameterTypes.iter()) {
        let arg = transformExpr(
            pstate,
            Some((**larg).clone_in(mcx)?),
            ParseExprKind::EXPR_KIND_FROM_FUNCTION,
        )?
        .ok_or_else(|| elog_error("transformRangeTableSample: transformExpr returned NULL"))?;
        let mut arg: Expr<'mcx> =
            coerce_to_specific_type(mcx, Some(pstate), arg, argtype, "TABLESAMPLE")?.clone_in(mcx)?;
        assign_expr_collations(Some(pstate), &mut arg)?;
        fargs.try_reserve(1).map_err(|_| mcx.oom(0))?;
        fargs.push(arg);
    }
    tablesample.args = Some(fargs);

    /* Process REPEATABLE (seed) */
    if let Some(repeatable) = rts.repeatable.as_deref() {
        if !tsm.repeatable_across_queries {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "tablesample method {} does not support REPEATABLE",
                    name_list_to_string(&rts.method)
                ))
                .errposition(errpos(pstate, rts.location))
                .into_error());
        }

        let arg = transformExpr(
            pstate,
            Some(repeatable.clone_in(mcx)?),
            ParseExprKind::EXPR_KIND_FROM_FUNCTION,
        )?
        .ok_or_else(|| elog_error("transformRangeTableSample: transformExpr returned NULL"))?;
        let mut arg: Expr<'mcx> =
            coerce_to_specific_type(mcx, Some(pstate), arg, FLOAT8OID, "REPEATABLE")?.clone_in(mcx)?;
        assign_expr_collations(Some(pstate), &mut arg)?;
        tablesample.repeatable = Some(alloc::boxed::Box::new(arg));
    } else {
        tablesample.repeatable = None;
    }

    Ok(tablesample)
}

// ===========================================================================
// getNSItemForSpecialRelationTypes — parse_clause.c:1010
// ===========================================================================

/// If given RangeVar refers to a CTE or an EphemeralNamedRelation, build and
/// return an appropriate ParseNamespaceItem, otherwise return None.
fn getNSItemForSpecialRelationTypes<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    rv: &RangeVar<'mcx>,
) -> PgResult<Option<ParseNamespaceItem<'mcx>>> {
    /*
     * if it is a qualified name, it can't be a CTE or tuplestore reference
     */
    if rv.schemaname.is_some() {
        return Ok(None);
    }

    let relname = rv.relname.as_deref().unwrap_or("");
    if let Some((mut cte, levelsup)) = scanNameSpaceForCTE(mcx, pstate, relname)? {
        // C: addRangeTableEntryForCTE(pstate, cte, levelsup, rv, true). The
        // owner mutates the CTE copy that scanNameSpaceForCTE returned.
        let nsitem = addRangeTableEntryForCTE(mcx, pstate, &mut cte, levelsup, rv, true)?;
        Ok(Some(nsitem))
    } else if scanNameSpaceForENR(pstate, relname) {
        Ok(Some(addRangeTableEntryForENR(mcx, pstate, rv, true)?))
    } else {
        Ok(None)
    }
}

// ===========================================================================
// transformFromClauseItem — parse_clause.c:1053
// ===========================================================================

/// Transform a FROM-clause item, adding any required entries to the range
/// table list being built in the ParseState, and return the transformed item
/// ready to include in the joinlist plus the namespace it exposes.
///
/// The return value's `Node` is the jointree node (RangeTblRef or JoinExpr).
/// The returned `Vec`'s LAST element is the C `*top_nsitem`.
fn transformFromClauseItem<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    n: &Node<'mcx>,
) -> PgResult<(Node<'mcx>, Vec<ParseNamespaceItem<'mcx>>)> {
    /* Guard against stack overflow due to overly deep subtree */
    /* check_stack_depth() handled by the host runtime */

    match n.node_tag() {
        ntag::T_RangeVar => {
            let rv = n.expect_rangevar();
            /* Plain relation reference, or perhaps a CTE reference */

            /* Check if it's a CTE or tuplestore reference */
            let nsitem = match getNSItemForSpecialRelationTypes(mcx, pstate, rv)? {
                Some(ns) => ns,
                /* if not found above, must be a table reference */
                None => transformTableEntry(mcx, pstate, rv)?,
            };

            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?;
            Ok((rtr, namespace))
        }
        ntag::T_RangeSubselect => {
            let rs = n.expect_rangesubselect();
            /* sub-SELECT is like a plain relation */
            let nsitem = transformRangeSubselect(mcx, pstate, rs)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?;
            Ok((rtr, namespace))
        }
        ntag::T_RangeFunction => {
            let rf = n.expect_rangefunction();
            /* function is like a plain relation */
            let nsitem = transformRangeFunction(mcx, pstate, rf)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?;
            Ok((rtr, namespace))
        }
        /*
         * `IsA(n, RangeTableFunc)` (parse_clause.c:1107) — XMLTABLE. (The
         * combined C arm also dispatches `JsonTable` → `transformJsonTable`,
         * which is deferred; `T_JsonTable` falls through to the default below
         * until the JSON_TABLE subsystem lands.)
         */
        ntag::T_RangeTableFunc => {
            let rtf = n.expect_rangetablefunc();
            let nsitem = transformRangeTableFunc(mcx, pstate, rtf)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?;
            Ok((rtr, namespace))
        }
        ntag::T_JsonTable => {
            let jt = n.expect_jsontable();
            let nsitem = transformJsonTable(mcx, pstate, jt)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex })?;
            Ok((rtr, namespace))
        }
        ntag::T_RangeTableSample => {
            let rts = n.expect_rangetablesample();
            /* TABLESAMPLE clause (wrapping some other valid FROM node) */

            /* Recursively transform the contained relation */
            let rel_inner = rts
                .relation
                .as_deref()
                .ok_or_else(|| elog_error("transformFromClauseItem: TABLESAMPLE relation is NULL"))?;
            let (rel, namespace) = transformFromClauseItem(mcx, pstate, rel_inner)?;

            /*
             * top_nsitem == last element of namespace; its p_rte's rtindex
             * is the live RTE in pstate.p_rtable that TABLESAMPLE attaches to.
             */
            let top_nsitem = namespace
                .last()
                .ok_or_else(|| elog_error("transformFromClauseItem: empty TABLESAMPLE namespace"))?;
            let top_rtindex = top_nsitem.p_rtindex;
            let rte = &pstate.p_rtable[(top_rtindex - 1) as usize];

            /* We only support this on plain relations and matviews */
            if rte.rtekind != RTEKind::RTE_RELATION
                || (rte.relkind != RELKIND_RELATION
                    && rte.relkind != RELKIND_MATVIEW
                    && rte.relkind != RELKIND_PARTITIONED_TABLE)
            {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(
                        "TABLESAMPLE clause can only be applied to tables and materialized views",
                    )
                    .errposition(errpos(pstate, node_location(rel_inner)?))
                    .into_error());
            }

            /* Transform TABLESAMPLE details and attach to the RTE */
            let tablesample = transformRangeTableSample(mcx, pstate, rts)?;
            pstate.p_rtable[(top_rtindex - 1) as usize].tablesample =
                Some(alloc_in(mcx, Node::mk_table_sample_clause(mcx, tablesample)?)?);

            Ok((rel, namespace))
        }
        ntag::T_JoinExpr => {
            let j = n.expect_joinexpr();
            transform_from_clause_item_join(mcx, pstate, j)
        }
        _ => Err(ereport(ERROR)
            .errmsg(format!("unrecognized node type: {:?}", n.node_tag()))
            .into_error()),
    }
}

/// The `IsA(n, JoinExpr)` arm of `transformFromClauseItem`
/// (parse_clause.c:1158-1465), 1:1.
fn transform_from_clause_item_join<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    j_in: &JoinExpr<'mcx>,
) -> PgResult<(Node<'mcx>, Vec<ParseNamespaceItem<'mcx>>)> {
    let mut j = j_in.clone_in(mcx)?;

    /*
     * Recursively process the left subtree, then the right.  We must do it in
     * this order for correct visibility of LATERAL references.
     */
    let larg = j
        .larg
        .as_deref()
        .ok_or_else(|| elog_error("transformFromClauseItem: JoinExpr without larg"))?;
    let (l_item, mut l_namespace) = transformFromClauseItem(mcx, pstate, larg)?;
    j.larg = Some(alloc_in(mcx, l_item.clone_in(mcx)?)?);
    let l_nsitem = clone_nsitem(
        mcx,
        l_namespace
            .last()
            .ok_or_else(|| elog_error("transformFromClauseItem: empty left namespace"))?,
    )?;

    /*
     * Make the left-side RTEs available for LATERAL access within the right
     * side, by temporarily adding them to the pstate's namespace list.
     */
    let lateral_ok = j.jointype == JoinType::JOIN_INNER || j.jointype == JoinType::JOIN_LEFT;
    setNamespaceLateralState(&mut l_namespace, true, lateral_ok);

    let sv_namespace_length = pstate.p_namespace.len();
    for nsitem in l_namespace.iter() {
        pstate.p_namespace.try_reserve(1).map_err(|_| mcx.oom(0))?;
        pstate.p_namespace.push(clone_nsitem(mcx, nsitem)?);
    }

    /* And now we can process the RHS */
    let rarg = j
        .rarg
        .as_deref()
        .ok_or_else(|| elog_error("transformFromClauseItem: JoinExpr without rarg"))?;
    let (r_item, r_namespace) = transformFromClauseItem(mcx, pstate, rarg)?;
    j.rarg = Some(alloc_in(mcx, r_item.clone_in(mcx)?)?);
    let r_nsitem = clone_nsitem(
        mcx,
        r_namespace
            .last()
            .ok_or_else(|| elog_error("transformFromClauseItem: empty right namespace"))?,
    )?;

    /* Remove the left-side RTEs from the namespace list again */
    pstate.p_namespace.truncate(sv_namespace_length);

    /*
     * Check for conflicting refnames in left and right subtrees.
     */
    checkNameSpaceConflicts(pstate, &l_namespace, &r_namespace)?;

    /*
     * Generate combined namespace info for possible use below.
     */
    let mut my_namespace: Vec<ParseNamespaceItem<'mcx>> = Vec::new();
    for ns in l_namespace.into_iter() {
        my_namespace.push(ns);
    }
    for ns in r_namespace.into_iter() {
        my_namespace.push(ns);
    }

    /*
     * We'll work from the nscolumns data and eref alias column names for each
     * of the input nsitems.
     */
    let l_nscolumns: Vec<ParseNamespaceColumn> = l_nsitem.p_nscolumns.iter().copied().collect();
    let l_colnames: Vec<String> = nsitem_colnames(&l_nsitem);
    let r_nscolumns: Vec<ParseNamespaceColumn> = r_nsitem.p_nscolumns.iter().copied().collect();
    let r_colnames: Vec<String> = nsitem_colnames(&r_nsitem);

    /*
     * Natural join does not explicitly specify columns; must generate columns
     * to join.
     */
    if j.isNatural {
        debug_assert!(j.usingClause.is_empty()); /* shouldn't have USING() too */

        let mut rlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

        for l_colname in l_colnames.iter() {
            if l_colname.is_empty() {
                continue; /* ignore dropped columns */
            }
            let mut m_name: Option<&str> = None;
            for r_colname in r_colnames.iter() {
                if l_colname == r_colname {
                    m_name = Some(l_colname.as_str());
                    break;
                }
            }
            /* matched a right column? then keep as join column... */
            if let Some(name) = m_name {
                rlist.try_reserve(1).map_err(|_| mcx.oom(0))?;
                rlist.push(make_string_node(mcx, name)?);
            }
        }

        j.usingClause = rlist;
    }

    /*
     * If a USING clause alias was specified, save the USING columns as its
     * column list.
     */
    if let Some(jua) = j.join_using_alias.as_deref_mut() {
        jua.colnames = copy_node_pgvec(mcx, &j.usingClause)?;
    }

    /* Convenience view of the USING column names. */
    let using_names: Vec<String> = name_list_strings(&j.usingClause);

    /*
     * Now transform the join qualifications, if any.
     */
    let mut l_colnos: Vec<i32> = Vec::new();
    let mut r_colnos: Vec<i32> = Vec::new();
    let mut res_colnames: Vec<String> = Vec::new();
    let mut res_colvars: Vec<Node<'mcx>> = Vec::new();
    let mut res_nscolumns: Vec<ParseNamespaceColumn> = Vec::new();

    if !j.usingClause.is_empty() {
        /*
         * JOIN/USING (or NATURAL JOIN, as transformed above). Transform the
         * list into an explicit ON-condition.
         */
        debug_assert!(j.quals.is_none()); /* shouldn't have ON() too */

        let mut l_usingvars: Vec<Var> = Vec::new();
        let mut r_usingvars: Vec<Var> = Vec::new();

        for u_colname in using_names.iter() {
            debug_assert!(!u_colname.is_empty());

            /* Check for USING(foo,foo) */
            if res_colnames.iter().any(|c| c == u_colname) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!(
                        "column name \"{u_colname}\" appears more than once in USING clause"
                    ))
                    .into_error());
            }

            /* Find it in left input */
            let mut l_index: i32 = -1;
            for (ndx, l_colname) in l_colnames.iter().enumerate() {
                if l_colname == u_colname {
                    if l_index >= 0 {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                            .errmsg(format!(
                                "common column name \"{u_colname}\" appears more than once in left table"
                            ))
                            .into_error());
                    }
                    l_index = ndx as i32;
                }
            }
            if l_index < 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{u_colname}\" specified in USING clause does not exist in left table"
                    ))
                    .into_error());
            }
            l_colnos.push(l_index + 1);

            /* Find it in right input */
            let mut r_index: i32 = -1;
            for (ndx, r_colname) in r_colnames.iter().enumerate() {
                if r_colname == u_colname {
                    if r_index >= 0 {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                            .errmsg(format!(
                                "common column name \"{u_colname}\" appears more than once in right table"
                            ))
                            .into_error());
                    }
                    r_index = ndx as i32;
                }
            }
            if r_index < 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{u_colname}\" specified in USING clause does not exist in right table"
                    ))
                    .into_error());
            }
            r_colnos.push(r_index + 1);

            /* Build Vars to use in the generated JOIN ON clause */
            l_usingvars.push(buildVarFromNSColumn(pstate, &l_nscolumns[l_index as usize])?);
            r_usingvars.push(buildVarFromNSColumn(pstate, &r_nscolumns[r_index as usize])?);

            /*
             * While we're here, add column names to the res_colnames list.
             */
            res_colnames.push(u_colname.clone());
        }

        /* Construct the generated JOIN ON clause */
        let quals = transformJoinUsingClause(mcx, pstate, &l_usingvars, &r_usingvars)?;
        j.quals = Some(alloc_in(mcx, Node::mk_expr(mcx, quals.clone_in(mcx)?)?)?);
    } else if j.quals.is_some() {
        /* User-written ON-condition; transform it */
        let quals_node = j
            .quals
            .as_deref()
            .unwrap()
            .clone_in(mcx)?;
        let quals = transformJoinOnClause(mcx, pstate, quals_node, &mut my_namespace)?;
        j.quals = Some(alloc_in(mcx, Node::mk_expr(mcx, quals.clone_in(mcx)?)?)?);
    } else {
        /* CROSS JOIN: no quals */
    }

    /*
     * If this is an outer join, now mark the appropriate child RTEs as being
     * nulled by this join.
     */
    j.rtindex = (pstate.p_rtable.len() + 1) as i32;

    match j.jointype {
        JoinType::JOIN_INNER => {}
        JoinType::JOIN_LEFT => {
            let rarg = j.rarg.as_deref().unwrap().clone_in(mcx)?;
            markRelsAsNulledBy(mcx, pstate, &rarg, j.rtindex)?;
        }
        JoinType::JOIN_FULL => {
            let larg = j.larg.as_deref().unwrap().clone_in(mcx)?;
            let rarg = j.rarg.as_deref().unwrap().clone_in(mcx)?;
            markRelsAsNulledBy(mcx, pstate, &larg, j.rtindex)?;
            markRelsAsNulledBy(mcx, pstate, &rarg, j.rtindex)?;
        }
        JoinType::JOIN_RIGHT => {
            let larg = j.larg.as_deref().unwrap().clone_in(mcx)?;
            markRelsAsNulledBy(mcx, pstate, &larg, j.rtindex)?;
        }
        /* shouldn't see any other types here */
        other => {
            return Err(elog_error(format!("unrecognized join type: {}", other as i32)));
        }
    }

    /*
     * Now we can construct join alias expressions for the USING columns.
     */
    if !j.usingClause.is_empty() {
        /* Scan the colnos lists to recover info from the previous loop */
        for (lc1, lc2) in l_colnos.iter().zip(r_colnos.iter()) {
            let l_index = (*lc1 - 1) as usize;
            let r_index = (*lc2 - 1) as usize;

            /*
             * Note we re-build these Vars: they might have different
             * varnullingrels than the ones made in the previous loop.
             */
            let l_colvar = buildVarFromNSColumn(pstate, &l_nscolumns[l_index])?;
            let r_colvar = buildVarFromNSColumn(pstate, &r_nscolumns[r_index])?;

            /* Construct the join alias Var for this column */
            let (u_colvar, which) =
                buildMergedJoinVar(mcx, pstate, j.jointype, &l_colvar, &r_colvar)?;
            res_colvars.push(u_colvar.clone_in(mcx)?);

            /* Construct column's res_nscolumns[] entry */
            let res_colindex = (res_nscolumns.len() + 1) as i32; /* 1-based, post-push */
            let entry = match which {
                /* Merged column is equivalent to left input */
                MergedWhich::Left => l_nscolumns[l_index],
                /* Merged column is equivalent to right input */
                MergedWhich::Right => r_nscolumns[r_index],
                /*
                 * Merged column is not semantically equivalent to either
                 * input, so it needs to be referenced as the join output
                 * column.
                 */
                MergedWhich::New => {
                    let u_expr = node_as_expr(&u_colvar);
                    ParseNamespaceColumn {
                        p_varno: j.rtindex as Index,
                        p_varattno: res_colindex as AttrNumber,
                        p_vartype: expr_type(u_expr)?,
                        p_vartypmod: expr_typmod(u_expr)?,
                        p_varcollid: expr_collation(u_expr)?,
                        p_varnosyn: j.rtindex as Index,
                        p_varattnosyn: res_colindex as AttrNumber,
                        ..ParseNamespaceColumn::default()
                    }
                }
            };
            res_nscolumns.push(entry);
        }
    }

    /* Add remaining columns from each side to the output columns */
    extractRemainingColumns(
        pstate,
        &l_nscolumns,
        &l_colnames,
        &mut l_colnos,
        &mut res_colnames,
        &mut res_colvars,
        &mut res_nscolumns,
    )?;
    extractRemainingColumns(
        pstate,
        &r_nscolumns,
        &r_colnames,
        &mut r_colnos,
        &mut res_colnames,
        &mut res_colvars,
        &mut res_nscolumns,
    )?;

    /* If join has an alias, it syntactically hides all inputs */
    if j.alias.is_some() {
        for (k, nscol) in res_nscolumns.iter_mut().enumerate() {
            nscol.p_varnosyn = j.rtindex as Index;
            nscol.p_varattnosyn = (k + 1) as AttrNumber;
        }
    }

    /*
     * Now build an RTE and nsitem for the result of the join.
     */
    let res_colindex = res_nscolumns.len();
    let res_colnames_nodes = strings_to_node_pgvec(mcx, &res_colnames)?;
    let res_nscolumns_pgvec = nscolumns_to_pgvec(mcx, &res_nscolumns)?;
    let res_colvars_pgvec = nodes_to_pgvec(mcx, res_colvars)?;
    let l_colnos_pgvec = ints_to_pgvec(mcx, &l_colnos)?;
    let r_colnos_pgvec = ints_to_pgvec(mcx, &r_colnos)?;
    let nummergedcols = j.usingClause.len() as i32;

    let mut nsitem = addRangeTableEntryForJoin(
        mcx,
        pstate,
        &res_colnames_nodes,
        res_nscolumns_pgvec,
        j.jointype,
        nummergedcols,
        res_colvars_pgvec,
        l_colnos_pgvec,
        r_colnos_pgvec,
        copy_opt_alias(mcx, j.join_using_alias.as_deref())?,
        copy_opt_alias(mcx, j.alias.as_deref())?,
        true,
    )?;

    /* Verify that we correctly predicted the join's RT index */
    debug_assert_eq!(j.rtindex, nsitem.p_rtindex);
    /* Cross-check number of columns, too */
    debug_assert_eq!(res_colindex, nsitem_colname_count(&nsitem));

    /*
     * Save a link to the JoinExpr in the proper element of p_joinexprs.
     * Since we maintain that list lazily, it may be necessary to fill in
     * empty entries before we can add the JoinExpr in the right place.
     */
    while (pstate.p_joinexprs.len() as i32) < j.rtindex - 1 {
        pstate.p_joinexprs.try_reserve(1).map_err(|_| mcx.oom(0))?;
        pstate.p_joinexprs.push(None);
    }
    pstate.p_joinexprs.try_reserve(1).map_err(|_| mcx.oom(0))?;
    pstate.p_joinexprs.push(Some(alloc_in(mcx, j.clone_in(mcx)?)?));
    debug_assert_eq!(pstate.p_joinexprs.len() as i32, j.rtindex);

    /*
     * If the join has a USING alias, build a ParseNamespaceItem for that and
     * add it to the list of nsitems in the join's input.
     */
    if j.join_using_alias.is_some() {
        let jnsitem = ParseNamespaceItem {
            p_names: match j.join_using_alias.as_deref() {
                Some(a) => Some(alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
            p_rte: match nsitem.p_rte.as_deref() {
                Some(r) => Some(alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            p_rtindex: nsitem.p_rtindex,
            p_perminfo: None,
            /* no need to copy the first N columns, just use res_nscolumns */
            p_nscolumns: clone_nscolumns_pgvec(mcx, &nsitem.p_nscolumns)?,
            /* set default visibility flags; might get changed later */
            p_rel_visible: true,
            p_cols_visible: true,
            p_lateral_only: false,
            p_lateral_ok: true,
            p_returning_type: VAR_RETURNING_DEFAULT,
        };
        /* Per SQL, we must check for alias conflicts */
        checkNameSpaceConflicts(pstate, core::slice::from_ref(&jnsitem), &my_namespace)?;
        my_namespace.push(jnsitem);
    }

    /*
     * Prepare returned namespace list.  If the JOIN has an alias then it
     * hides the contained RTEs completely; otherwise, the contained RTEs are
     * still visible as table names, but are not visible for unqualified
     * column-name access.
     */
    if j.alias.is_some() {
        my_namespace = Vec::new();
    } else {
        setNamespaceColumnVisibility(&mut my_namespace, false);
    }

    /*
     * The join RTE itself is always made visible for unqualified column
     * names.  It's visible as a relation name only if it has an alias.
     */
    nsitem.p_rel_visible = j.alias.is_some();
    nsitem.p_cols_visible = true;
    nsitem.p_lateral_only = false;
    nsitem.p_lateral_ok = true;

    /* C: *top_nsitem = nsitem; *namespace = lappend(my_namespace, nsitem). */
    my_namespace.push(nsitem);
    Ok((Node::mk_join_expr(mcx, j)?, my_namespace))
}

// ===========================================================================
// buildVarFromNSColumn — parse_clause.c:1639
// ===========================================================================

/// Build a Var node using ParseNamespaceColumn data (for joinaliasvars).
/// varlevelsup is 0, no location, no column SELECT privilege requested.
fn buildVarFromNSColumn(pstate: &ParseState<'_>, nscol: &ParseNamespaceColumn) -> PgResult<Var> {
    debug_assert!(nscol.p_varno > 0); /* i.e., not deleted column */
    let mut var = make_var(
        nscol.p_varno as i32,
        nscol.p_varattno,
        nscol.p_vartype,
        nscol.p_vartypmod,
        nscol.p_varcollid,
        0,
    );
    /* makeVar doesn't offer parameters for these, so set by hand: */
    var.varreturningtype = nscol.p_varreturningtype;
    var.varnosyn = nscol.p_varnosyn;
    var.varattnosyn = nscol.p_varattnosyn;

    /* ... and update varnullingrels */
    markNullableIfNeeded(pstate, &mut var)?;

    Ok(var)
}

// ===========================================================================
// buildMergedJoinVar — parse_clause.c:1666
// ===========================================================================

/// Which input the merged join column is semantically equivalent to — the
/// owned-tree carrier of C's `u_colvar == (Node *) l_colvar` /
/// `== (Node *) r_colvar` pointer-identity tests.
enum MergedWhich {
    /// `u_colvar == (Node *) l_colvar`.
    Left,
    /// `u_colvar == (Node *) r_colvar`.
    Right,
    /// A coercion/COALESCE wrapper — referenced as the join output column.
    New,
}

/// Generate a suitable replacement expression for a merged join column, plus
/// the which-input identity marker.
fn buildMergedJoinVar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    jointype: JoinType,
    l_colvar: &Var,
    r_colvar: &Var,
) -> PgResult<(Node<'mcx>, MergedWhich)> {
    let both = [Expr::Var(l_colvar.clone()), Expr::Var(r_colvar.clone())];

    let outcoltype = select_common_type(Some(pstate), &both, Some("JOIN/USING"))?;
    let outcoltypmod = select_common_typmod(&both, outcoltype)?;

    /*
     * Insert coercion functions if needed.  Note that a difference in typmod
     * can only happen if input has typmod but outcoltypmod is -1. In that
     * case we insert a RelabelType.  We never need coerce_type_typmod.
     * `l_is_var` carries "the produced node IS the input Var" (C pointer id).
     */
    let (l_node, l_is_var): (Expr<'mcx>, bool) = if l_colvar.vartype != outcoltype {
        let coerced = coerce_type(
            mcx,
            Some(pstate),
            Some(Expr::Var(l_colvar.clone())),
            l_colvar.vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| elog_error("buildMergedJoinVar: coerce_type returned NULL"))?;
        // coerce_type yields the parser-arena `'static`; bring it into `mcx` so
        // it joins `make_relabel_type`/`Expr::Var` results at the arena lifetime.
        (coerced.clone_in(mcx)?, false)
    } else if l_colvar.vartypmod != outcoltypmod {
        (
            make_relabel_type(
                Expr::Var(l_colvar.clone()),
                outcoltype,
                outcoltypmod,
                InvalidOid, /* fixed below */
                COERCE_IMPLICIT_CAST,
            ),
            false,
        )
    } else {
        (Expr::Var(l_colvar.clone()), true)
    };

    let (r_node, r_is_var): (Expr<'mcx>, bool) = if r_colvar.vartype != outcoltype {
        let coerced = coerce_type(
            mcx,
            Some(pstate),
            Some(Expr::Var(r_colvar.clone())),
            r_colvar.vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?
        .ok_or_else(|| elog_error("buildMergedJoinVar: coerce_type returned NULL"))?;
        (coerced.clone_in(mcx)?, false)
    } else if r_colvar.vartypmod != outcoltypmod {
        (
            make_relabel_type(
                Expr::Var(r_colvar.clone()),
                outcoltype,
                outcoltypmod,
                InvalidOid, /* fixed below */
                COERCE_IMPLICIT_CAST,
            ),
            false,
        )
    } else {
        (Expr::Var(r_colvar.clone()), true)
    };

    /*
     * Choose what to emit
     */
    let (mut res_node, which): (Expr<'mcx>, MergedWhich) = match jointype {
        JoinType::JOIN_INNER => {
            /* We can use either var; prefer non-coerced one if available. */
            if l_is_var {
                (l_node, MergedWhich::Left)
            } else if r_is_var {
                (r_node, MergedWhich::Right)
            } else {
                (l_node, MergedWhich::New)
            }
        }
        JoinType::JOIN_LEFT => {
            /* Always use left var */
            let w = if l_is_var { MergedWhich::Left } else { MergedWhich::New };
            (l_node, w)
        }
        JoinType::JOIN_RIGHT => {
            /* Always use right var */
            let w = if r_is_var { MergedWhich::Right } else { MergedWhich::New };
            (r_node, w)
        }
        JoinType::JOIN_FULL => {
            /*
             * Here we must build a COALESCE expression to ensure that the
             * join output is non-null if either input is.
             */
            let c = ::nodes::primnodes::CoalesceExpr {
                coalescetype: outcoltype,
                /* coalescecollid will get set below */
                coalescecollid: InvalidOid,
                args: alloc::vec![l_node, r_node],
                location: -1,
            };
            (Expr::CoalesceExpr(c), MergedWhich::New)
        }
        other => {
            return Err(elog_error(format!("unrecognized join type: {}", other as i32)));
        }
    };

    /*
     * Apply assign_expr_collations to fix up the collation info in the
     * coercion and CoalesceExpr nodes, if we made any.
     */
    assign_expr_collations(Some(pstate), &mut res_node)?;

    Ok((Node::mk_expr(mcx, res_node)?, which))
}

// ===========================================================================
// markRelsAsNulledBy — parse_clause.c:1774
// ===========================================================================

/// Mark the given jointree node and its children as nulled by join jindex.
fn markRelsAsNulledBy<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    n: &Node<'mcx>,
    jindex: i32,
) -> PgResult<()> {
    /* Note: we can't see FromExpr here */
    let varno: i32 = match n.node_tag() {
        ntag::T_RangeTblRef => n.expect_rangetblref().rtindex,
        ntag::T_JoinExpr => {
            let j = n.expect_joinexpr();
            /* recurse to children */
            if let Some(l) = j.larg.as_deref() {
                let l = l.clone_in(mcx)?;
                markRelsAsNulledBy(mcx, pstate, &l, jindex)?;
            }
            if let Some(r) = j.rarg.as_deref() {
                let r = r.clone_in(mcx)?;
                markRelsAsNulledBy(mcx, pstate, &r, jindex)?;
            }
            j.rtindex
        }
        _ => {
            return Err(elog_error(format!(
                "unrecognized node type: {:?}",
                n.node_tag()
            )));
        }
    };

    /*
     * Now add jindex to the p_nullingrels set for relation varno.  Since we
     * maintain the p_nullingrels list lazily, we might need to extend it to
     * make the varno'th entry exist (C: lappend(..., NULL); the owned-tree
     * NULL cell is an empty `Bitmapset`).
     */
    while (pstate.p_nullingrels.len() as i32) < varno {
        pstate.p_nullingrels.try_reserve(1).map_err(|_| mcx.oom(0))?;
        pstate
            .p_nullingrels
            .push(empty_bitmapset(mcx)?);
    }
    /* lfirst(lc) = bms_add_member((Bitmapset *) lfirst(lc), jindex) */
    let idx = (varno - 1) as usize;
    let cur = core::mem::replace(&mut pstate.p_nullingrels[idx], empty_bitmapset(mcx)?);
    let cur_opt: Option<::mcx::PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>> =
        if cur.words.is_empty() {
            None
        } else {
            Some(alloc_in(mcx, cur)?)
        };
    let updated = ::nodes_core::bitmapset::bms_add_member(mcx, cur_opt, jindex)?;
    pstate.p_nullingrels[idx] = updated.clone_in(mcx)?;
    Ok(())
}

/// An empty `Bitmapset` value (the C `NULL` / lazy-list NULL cell).
fn empty_bitmapset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::nodes::bitmapset::Bitmapset<'mcx>> {
    Ok(::nodes::bitmapset::Bitmapset {
        words: PgVec::new_in(mcx),
    })
}

// ===========================================================================
// setNamespaceColumnVisibility — parse_clause.c:1815
// ===========================================================================

/// Convenience subroutine to update cols_visible flags in a namespace list.
fn setNamespaceColumnVisibility(namespace: &mut [ParseNamespaceItem<'_>], cols_visible: bool) {
    for nsitem in namespace.iter_mut() {
        nsitem.p_cols_visible = cols_visible;
    }
}

// ===========================================================================
// setNamespaceLateralState — parse_clause.c:1832
// ===========================================================================

/// Convenience subroutine to update LATERAL flags in a namespace list.
pub fn setNamespaceLateralState(
    namespace: &mut [ParseNamespaceItem<'_>],
    lateral_only: bool,
    lateral_ok: bool,
) {
    for nsitem in namespace.iter_mut() {
        nsitem.p_lateral_only = lateral_only;
        nsitem.p_lateral_ok = lateral_ok;
    }
}

// ===========================================================================
// Internal helpers (owned-model marshalling).
// ===========================================================================

/// A `String` value node (C `makeString(str)`) as an owned `Node`.
fn make_string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    alloc_in(
        mcx,
        Node::mk_string(mcx, ::nodes::value::StringNode {
            sval: ::mcx::PgString::from_str_in(s, mcx)?,
        })?,
    )
}

/// `SystemFuncName("unnest")` — `list_make2(makeString("pg_catalog"),
/// makeString(name))` (parser/parse_func.c).
fn system_func_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 2)?;
    v.push(make_string_node(mcx, "pg_catalog")?);
    v.push(make_string_node(mcx, name)?);
    Ok(v)
}

/// `nodeTag(n)`-based `exprLocation` over a raw FROM-relation node — only the
/// arms the TABLESAMPLE error path can reach (the contained relation node);
/// delegate to the F1 raw-node `exprLocation` via [`expr_location`] for typed
/// leaves. The TABLESAMPLE arm needs `exprLocation(rts->relation)` where the
/// relation is a RangeVar / RangeSubselect / RangeFunction / nested
/// RangeTableSample / JoinExpr; none of these carry a `location` in the
/// repo's structs except RangeVar / RangeTableSample, so this mirrors the C
/// `exprLocation` `default: loc = -1` for the others (C reads the same
/// missing-location fields as -1).
fn node_location(node: &Node<'_>) -> PgResult<i32> {
    let loc = match node.node_tag() {
        ntag::T_RangeVar => node.expect_rangevar().location,
        ntag::T_RangeTableSample => node.expect_rangetablesample().location,
        _ => match node.as_expr() {
            Some(e) => expr_location(Some(e))?,
            None => -1,
        },
    };
    Ok(loc)
}

/// `exprLocation((Node *) coldeflist)` — the C reads the list's leftmost
/// member location; the coldeflist members are `ColumnDef` nodes which carry
/// no location in the repo's struct, so this mirrors the C `-1` outcome (an
/// empty/locationless list).
fn list_node_location(_list: &PgVec<'_, NodePtr<'_>>) -> PgResult<i32> {
    Ok(-1)
}

/// `pstate->p_last_srf != last_srf && pstate->p_last_srf != newfexpr` — the
/// SRF-at-top-level check shared by both `transformRangeFunction` branches.
/// `last_srf` / the current `p_last_srf` are compared by structural equality
/// (the C compares pointers; in the owned tree the same node is the same
/// value).
fn check_srf_top_level<'mcx>(
    pstate: &ParseState<'mcx>,
    last_srf: Option<&Node<'mcx>>,
    newfexpr: &Node<'mcx>,
) -> PgResult<()> {
    let cur = pstate.p_last_srf.as_deref();
    let changed = match (cur, last_srf) {
        (None, None) => false,
        (Some(_), None) | (None, Some(_)) => true,
        (Some(a), Some(b)) => !nodes_ptr_eq(a, b),
    };
    let cur_is_new = match cur {
        Some(c) => nodes_ptr_eq(c, newfexpr),
        None => false,
    };
    if changed && !cur_is_new {
        let loc = match pstate.p_last_srf.as_deref() {
            Some(e) => node_location(e)?,
            None => -1,
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("set-returning functions must appear at top level of FROM")
            .errposition(errpos(pstate, loc))
            .into_error());
    }
    Ok(())
}

/// Models the C pointer-equality SRF check (`pstate->p_last_srf == fexpr`).
///
/// The C compares two `Node *` pointers, asking "did the SRF that bubbled up
/// out of `transformExpr` end up being the very node we're examining?" In the
/// owned tree there are no shared pointers: `p_last_srf` is set to an
/// independent *clone* of the transformed result (`set_p_last_srf` clones), and
/// the funcexpr handed to the check is a separately-allocated `Node::Expr`
/// wrapping the same transformed expression. Pointer identity therefore never
/// holds and would spuriously reject a legitimate top-level SRF
/// (`SELECT * FROM generate_series(1,3)`). Structural equality (`equal()`,
/// equalfuncs.c) is the faithful proxy: two clones of the same node are equal,
/// and the same pointer is trivially equal.
fn nodes_ptr_eq<'n>(a: &Node<'n>, b: &Node<'n>) -> bool {
    core::ptr::eq(a as *const _, b as *const _)
        || equalfuncs_seams::equal_node::call(a, b)
}

/// `(Node *) expr` viewed for `exprType`/etc. — wraps a borrowed [`Node::Expr`].
fn node_as_expr<'a, 'mcx>(node: &'a Node<'mcx>) -> Option<&'a Expr<'mcx>> {
    node.as_expr()
}

/// Convert a `List *` of `String` value nodes into owned `Vec<String>` (the
/// non-string / A_Star members map to `""`, mirroring `strVal` on a dropped
/// alias column-name slot).
fn name_list_strings(list: &PgVec<'_, NodePtr<'_>>) -> Vec<String> {
    let mut out = Vec::with_capacity(list.len());
    for n in list.iter() {
        out.push(String::from(str_val(n).unwrap_or("")));
    }
    out
}

/// `NameListToString(names)` for an error message: join the `String` parts with
/// `.`, rendering `A_Star`/non-string as `*`.
fn name_list_to_string(list: &PgVec<'_, NodePtr<'_>>) -> String {
    let mut s = String::new();
    for (i, n) in list.iter().enumerate() {
        if i != 0 {
            s.push('.');
        }
        match str_val(n) {
            Some(name) => s.push_str(name),
            None => s.push('*'),
        }
    }
    s
}

/// Column names of a namespace item's `Alias` (`nsitem->p_names->colnames`).
fn nsitem_colnames(nsitem: &ParseNamespaceItem<'_>) -> Vec<String> {
    match nsitem.p_names.as_deref() {
        Some(a) => name_list_strings(&a.colnames),
        None => Vec::new(),
    }
}

/// `list_length(nsitem->p_names->colnames)`.
fn nsitem_colname_count(nsitem: &ParseNamespaceItem<'_>) -> usize {
    nsitem
        .p_names
        .as_deref()
        .map(|a| a.colnames.len())
        .unwrap_or(0)
}

/// `copyObject(alias)` for an optional `Alias *`.
fn copy_opt_alias<'mcx>(
    mcx: Mcx<'mcx>,
    alias: Option<&Alias<'_>>,
) -> PgResult<Option<Alias<'mcx>>> {
    match alias {
        Some(a) => Ok(Some(a.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// Clone an optional borrowed `Node`.
fn clone_opt_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Node<'_>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        Some(n) => Ok(Some(n.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// Deep-copy a `List *` into a fresh `PgVec`.
fn copy_node_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    list: &PgVec<'_, NodePtr<'_>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, list.len())?;
    for n in list.iter() {
        v.push(alloc_in(mcx, (**n).clone_in(mcx)?)?);
    }
    Ok(v)
}

/// Owned strings → a `List *` of `String` value nodes.
fn strings_to_node_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    strings: &[String],
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, strings.len())?;
    for s in strings.iter() {
        v.push(make_string_node(mcx, s)?);
    }
    Ok(v)
}

/// Move `Node`s into a `PgVec<NodePtr>`.
fn nodes_to_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: Vec<Node<'mcx>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, nodes.len())?;
    for n in nodes.into_iter() {
        v.push(alloc_in(mcx, n)?);
    }
    Ok(v)
}

/// `int` list → `PgVec<i32>` (`list_make_int` image).
fn ints_to_pgvec<'mcx>(mcx: Mcx<'mcx>, ints: &[i32]) -> PgResult<PgVec<'mcx, i32>> {
    let mut v = ::mcx::vec_with_capacity_in::<i32>(mcx, ints.len())?;
    for &i in ints.iter() {
        v.push(i);
    }
    Ok(v)
}

/// Owned `ParseNamespaceColumn` slice → `PgVec`.
fn nscolumns_to_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &[ParseNamespaceColumn],
) -> PgResult<PgVec<'mcx, ParseNamespaceColumn>> {
    let mut v = ::mcx::vec_with_capacity_in::<ParseNamespaceColumn>(mcx, cols.len())?;
    for c in cols.iter() {
        v.push(*c);
    }
    Ok(v)
}

/// Clone a `PgVec<ParseNamespaceColumn>`.
fn clone_nscolumns_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &PgVec<'_, ParseNamespaceColumn>,
) -> PgResult<PgVec<'mcx, ParseNamespaceColumn>> {
    let mut v = ::mcx::vec_with_capacity_in::<ParseNamespaceColumn>(mcx, cols.len())?;
    for c in cols.iter() {
        v.push(*c);
    }
    Ok(v)
}

/// Deep-copy a `ParseNamespaceItem` into `mcx`.
fn clone_nsitem<'mcx>(
    mcx: Mcx<'mcx>,
    nsitem: &ParseNamespaceItem<'_>,
) -> PgResult<ParseNamespaceItem<'mcx>> {
    Ok(ParseNamespaceItem {
        p_names: match nsitem.p_names.as_deref() {
            Some(a) => Some(alloc_in(mcx, a.clone_in(mcx)?)?),
            None => None,
        },
        p_rte: match nsitem.p_rte.as_deref() {
            Some(r) => Some(alloc_in(mcx, r.clone_in(mcx)?)?),
            None => None,
        },
        p_rtindex: nsitem.p_rtindex,
        p_perminfo: match nsitem.p_perminfo.as_deref() {
            Some(p) => Some(alloc_in(mcx, p.clone_in(mcx)?)?),
            None => None,
        },
        p_nscolumns: clone_nscolumns_pgvec(mcx, &nsitem.p_nscolumns)?,
        p_rel_visible: nsitem.p_rel_visible,
        p_cols_visible: nsitem.p_cols_visible,
        p_lateral_only: nsitem.p_lateral_only,
        p_lateral_ok: nsitem.p_lateral_ok,
        p_returning_type: nsitem.p_returning_type,
    })
}

/// Deep-copy a namespace `Vec`.
fn clone_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    namespace: &[ParseNamespaceItem<'_>],
) -> PgResult<PgVec<'mcx, ParseNamespaceItem<'mcx>>> {
    let mut v = ::mcx::vec_with_capacity_in::<ParseNamespaceItem<'mcx>>(mcx, namespace.len())?;
    for ns in namespace.iter() {
        v.push(clone_nsitem(mcx, ns)?);
    }
    Ok(v)
}

/// Borrowed `Expr` view of the transformed func expr nodes for
/// `assign_list_collations`, which mutates the `Expr`s in place.
fn funcexprs_to_expr_vec<'mcx>(mcx: Mcx<'mcx>, funcexprs: &[NodePtr<'_>]) -> PgResult<Vec<Expr<'mcx>>> {
    let mut v = Vec::with_capacity(funcexprs.len());
    for n in funcexprs.iter() {
        match n.as_expr() {
            Some(e) => v.push(e.clone_in(mcx)?),
            None => return Err(elog_error("transformRangeFunction: funcexpr is not a transformed Expr")),
        }
    }
    Ok(v)
}

/// Store collation-assigned `Expr`s back into the `funcexprs` node list.
fn store_back_funcexprs<'mcx>(
    mcx: Mcx<'mcx>,
    funcexprs: &mut [NodePtr<'mcx>],
    exprs: Vec<Expr<'mcx>>,
) -> PgResult<()> {
    debug_assert_eq!(funcexprs.len(), exprs.len());
    for (slot, e) in funcexprs.iter_mut().zip(exprs.into_iter()) {
        *slot = alloc_in(mcx, Node::mk_expr(mcx, e)?)?;
    }
    Ok(())
}

/// `(Node *) funcexprs` — the func-expr list wrapped as a `T_List` node for
/// `contain_vars_of_level`.
fn funcexprs_as_list_node<'mcx>(
    mcx: Mcx<'mcx>,
    funcexprs: &[NodePtr<'mcx>],
) -> PgResult<Node<'mcx>> {
    let mut v = ::mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, funcexprs.len())?;
    for n in funcexprs.iter() {
        v.push(alloc_in(mcx, (**n).clone_in(mcx)?)?);
    }
    Ok(Node::mk_list(mcx, v)?)
}

