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
//! `GetTsmRoutine` (nodeSamplescan — `backend-executor-nodeSamplescan-seams`),
//! `LookupFuncName` / `get_func_rettype` (parse_func/lsyscache —
//! `backend-commands-functioncmds-seams`), plus the F1 seams.
//! `addRangeTableEntryForFunction` (parse_relation; panics until funcapi lands).
//!
//! # Deferred to follow-on families (NOT in this crate)
//!
//!   * F3a: on-conflict, `transformWindowDefinitions`.
//!   * F3b: `transformRangeTableFunc` (XMLTABLE), `transformJsonTable`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgVec};

use types_core::{Index, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLESAMPLE_ARGUMENT, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use backend_utils_error::ereport;

use types_acl::acl::AclMode;
use types_storage::lock::{NoLock, RowExclusiveLock};

use types_tuple::heaptuple::{FLOAT8OID, INTERNALOID};

use types_core::primitive::AttrNumber;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsenodes::RTEKind;
use types_nodes::parsestmt::{
    ParseExprKind, ParseNamespaceColumn, ParseNamespaceItem, ParseState,
};
use types_nodes::nodesamplescan::TableSampleClause;
use types_nodes::primnodes::{
    AND_EXPR, CoercionForm, Expr, Var, VarReturningType,
};
use types_nodes::rawnodes::{
    A_Expr_Kind, Alias, JoinExpr, RangeFunction, RangeSubselect, RangeTableSample,
    RangeTblRef, RangeVar,
};

use types_parsenodes::CoercionContext;

use backend_nodes_core::makefuncs::{make_a_expr, make_func_call, make_relabel_type, make_var};
use backend_nodes_core::nodefuncs::{expr_collation, expr_location, expr_type, expr_typmod};

use backend_optimizer_util_vars::var::contain_vars_of_level;
use backend_parser_parse_expr::transformExpr;
use backend_parser_parse_collate::{assign_expr_collations, assign_list_collations};
use backend_parser_coerce::{coerce_to_specific_type, coerce_type, select_common_type, select_common_typmod};
use backend_parser_relation::{
    addNSItemToQuery, addRangeTableEntryForCTE, addRangeTableEntryForENR,
    addRangeTableEntryForFunction, addRangeTableEntryForJoin, addRangeTableEntryForRelation,
    addRangeTableEntryForSubquery, addRangeTableEntry, checkNameSpaceConflicts, isLockedRefname,
    markNullableIfNeeded, markVarForSelectPriv, parserOpenTable, scanNameSpaceForCTE,
    scanNameSpaceForENR,
};
use backend_access_table_table::table_close;

use backend_parser_parse_func_seams as parse_func;
use backend_executor_nodeSamplescan_seams as tsmapi;
use backend_parser_analyze_seams as analyze;
use backend_parser_target_seams as parse_target;
use backend_utils_cache_lsyscache_seams as lsyscache;

use types_samplescan::TsmRoutine;

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
            res_colvars.push(Node::Expr(Expr::Var(buildVarFromNSColumn(
                pstate,
                &src_nscolumns[(attnum - 1) as usize],
            )?)));
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
) -> PgResult<Expr> {
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
        let mut name = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 1)?;
        name.push(make_string_node(mcx, "=")?);
        let e = make_a_expr(
            A_Expr_Kind::AEXPR_OP,
            name,
            Some(alloc_in(mcx, Node::Expr(Expr::Var(lvar.clone())))?),
            Some(alloc_in(mcx, Node::Expr(Expr::Var(rvar.clone())))?),
            -1,
        );

        /* Prepare to combine into an AND clause, if multiple join columns */
        andargs.push(Node::A_Expr(e));
    }

    /* Only need an AND if there's more than one join column */
    let result: Node<'mcx> = if andargs.len() == 1 {
        andargs.pop().unwrap()
    } else {
        /*
         * makeBoolExpr(AND_EXPR, andargs, -1): the raw operator tree's args are
         * raw `A_Expr` nodes, so this is a raw `BoolExpr` carried as a `Node`.
         */
        let mut args = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, andargs.len())?;
        for a in andargs.into_iter() {
            args.push(alloc_in(mcx, a)?);
        }
        Node::BoolExpr(types_nodes::rawexprnodes::BoolExpr {
            boolop: AND_EXPR,
            args,
            location: -1,
        })
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
        backend_parser_coerce::coerce_to_boolean(mcx, Some(pstate), result, "JOIN/USING")?;

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
) -> PgResult<Expr> {
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
    let query = match &*query_node {
        Node::Query(q) => q.clone_in(mcx)?,
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
        let pair = match &**lc {
            Node::List(pair) => pair,
            _ => return Err(elog_error("transformRangeFunction: function item is not a List")),
        };
        debug_assert!(pair.len() == 2);
        let fexpr = &*pair[0];
        let coldeflist = match &*pair[1] {
            Node::List(l) => copy_node_pgvec(mcx, l)?,
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
        if let Node::FuncCall(fc) = fexpr {
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
                    let mut newfc_args = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 1)?;
                    newfc_args.push(alloc_in(mcx, (**arg).clone_in(mcx)?)?);
                    let newfc = make_func_call(
                        mcx,
                        newfc_name,
                        newfc_args,
                        COERCE_EXPLICIT_CALL,
                        fc.location,
                    )?;
                    let newfc_node = Node::FuncCall(newfc);

                    let newfexpr = transformExpr(
                        pstate,
                        Some(newfc_node.clone_in(mcx)?),
                        ParseExprKind::EXPR_KIND_FROM_FUNCTION,
                    )?
                    .ok_or_else(|| elog_error("transformRangeFunction: transformExpr returned NULL"))?;
                    let newfexpr_node = Node::Expr(newfexpr);

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
        let newfexpr_node = Node::Expr(newfexpr);

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
        let mut exprs = funcexprs_to_expr_vec(&funcexprs)?;
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
    let funcnames_strs: Vec<mcx::PgString<'mcx>> = {
        let mut v = Vec::with_capacity(funcnames.len());
        for n in funcnames.iter() {
            v.push(mcx::PgString::from_str_in(n, mcx)?);
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
    let method_names: Vec<mcx::PgString<'mcx>> = {
        let mut v = Vec::with_capacity(rts.method.len());
        for n in rts.method.iter() {
            v.push(mcx::PgString::from_str_in(str_val(n).unwrap_or(""), mcx)?);
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
    let tsm: mcx::PgBox<'mcx, TsmRoutine> = tsmapi::get_tsm_routine_oid::call(mcx, handler_oid)?;

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
    let mut fargs: PgVec<'mcx, Expr> = PgVec::new_in(mcx);
    for (larg, &argtype) in rts.args.iter().zip(tsm.parameterTypes.iter()) {
        let arg = transformExpr(
            pstate,
            Some((**larg).clone_in(mcx)?),
            ParseExprKind::EXPR_KIND_FROM_FUNCTION,
        )?
        .ok_or_else(|| elog_error("transformRangeTableSample: transformExpr returned NULL"))?;
        let mut arg = coerce_to_specific_type(mcx, Some(pstate), arg, argtype, "TABLESAMPLE")?;
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
        let mut arg = coerce_to_specific_type(mcx, Some(pstate), arg, FLOAT8OID, "REPEATABLE")?;
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

    match n {
        Node::RangeVar(rv) => {
            /* Plain relation reference, or perhaps a CTE reference */

            /* Check if it's a CTE or tuplestore reference */
            let nsitem = match getNSItemForSpecialRelationTypes(mcx, pstate, rv)? {
                Some(ns) => ns,
                /* if not found above, must be a table reference */
                None => transformTableEntry(mcx, pstate, rv)?,
            };

            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::RangeTblRef(RangeTblRef { rtindex });
            Ok((rtr, namespace))
        }
        Node::RangeSubselect(rs) => {
            /* sub-SELECT is like a plain relation */
            let nsitem = transformRangeSubselect(mcx, pstate, rs)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::RangeTblRef(RangeTblRef { rtindex });
            Ok((rtr, namespace))
        }
        Node::RangeFunction(rf) => {
            /* function is like a plain relation */
            let nsitem = transformRangeFunction(mcx, pstate, rf)?;
            let rtindex = nsitem.p_rtindex;
            let namespace = alloc::vec![nsitem];
            let rtr = Node::RangeTblRef(RangeTblRef { rtindex });
            Ok((rtr, namespace))
        }
        /*
         * `IsA(n, RangeTableFunc) || IsA(n, JsonTable)` (parse_clause.c:996) —
         * XMLTABLE / JSON_TABLE. The repo's central `Node` enum has no
         * `RangeTableFunc` / `JsonTable` arm yet, so the grammar cannot hand
         * one to this dispatcher; the matching transforms
         * (`transformRangeTableFunc` / `transformJsonTable`) are deferred to
         * F3b. There is therefore no reachable arm to write here.
         */
        Node::RangeTableSample(rts) => {
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
                Some(alloc_in(mcx, Node::TableSampleClause(tablesample))?);

            Ok((rel, namespace))
        }
        Node::JoinExpr(j) => transform_from_clause_item_join(mcx, pstate, j),
        other => Err(ereport(ERROR)
            .errmsg(format!("unrecognized node type: {:?}", other.node_tag()))
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
        j.quals = Some(alloc_in(mcx, Node::Expr(quals))?);
    } else if j.quals.is_some() {
        /* User-written ON-condition; transform it */
        let quals_node = j
            .quals
            .as_deref()
            .unwrap()
            .clone_in(mcx)?;
        let quals = transformJoinOnClause(mcx, pstate, quals_node, &mut my_namespace)?;
        j.quals = Some(alloc_in(mcx, Node::Expr(quals))?);
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
    Ok((Node::JoinExpr(j), my_namespace))
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
    let (l_node, l_is_var) = if l_colvar.vartype != outcoltype {
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
        (coerced, false)
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

    let (r_node, r_is_var) = if r_colvar.vartype != outcoltype {
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
        (coerced, false)
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
    let (mut res_node, which): (Expr, MergedWhich) = match jointype {
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
            let c = types_nodes::primnodes::CoalesceExpr {
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

    Ok((Node::Expr(res_node), which))
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
    let varno: i32 = match n {
        Node::RangeTblRef(r) => r.rtindex,
        Node::JoinExpr(j) => {
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
        other => {
            return Err(elog_error(format!(
                "unrecognized node type: {:?}",
                other.node_tag()
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
    let cur_opt: Option<mcx::PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>> =
        if cur.words.is_empty() {
            None
        } else {
            Some(alloc_in(mcx, cur)?)
        };
    let updated = backend_nodes_core::bitmapset::bms_add_member(mcx, cur_opt, jindex)?;
    pstate.p_nullingrels[idx] = updated.clone_in(mcx)?;
    Ok(())
}

/// An empty `Bitmapset` value (the C `NULL` / lazy-list NULL cell).
fn empty_bitmapset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<types_nodes::bitmapset::Bitmapset<'mcx>> {
    Ok(types_nodes::bitmapset::Bitmapset {
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
        Node::String(types_nodes::value::StringNode {
            sval: mcx::PgString::from_str_in(s, mcx)?,
        }),
    )
}

/// `SystemFuncName("unnest")` — `list_make2(makeString("pg_catalog"),
/// makeString(name))` (parser/parse_func.c).
fn system_func_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 2)?;
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
    let loc = match node {
        Node::RangeVar(rv) => rv.location,
        Node::RangeTableSample(rts) => rts.location,
        Node::Expr(e) => expr_location(Some(e))?,
        _ => -1,
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

/// Structural "is this the same node" used for the C pointer-equality SRF
/// check. Both operands are `p_last_srf` snapshots / freshly-produced FuncExpr
/// trees; comparing the rendered tag + a pointer-shaped identity is sufficient
/// for the C's "did a NEW srf appear" test (the C only ever asks whether the
/// pointer changed vs the saved one or equals the just-built expr).
fn nodes_ptr_eq(a: &Node<'_>, b: &Node<'_>) -> bool {
    core::ptr::eq(a as *const _, b as *const _)
}

/// `(Node *) expr` viewed for `exprType`/etc. — wraps a borrowed [`Node::Expr`].
fn node_as_expr<'a>(node: &'a Node<'_>) -> Option<&'a Expr> {
    match node {
        Node::Expr(e) => Some(e),
        _ => None,
    }
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
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, list.len())?;
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
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, strings.len())?;
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
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, nodes.len())?;
    for n in nodes.into_iter() {
        v.push(alloc_in(mcx, n)?);
    }
    Ok(v)
}

/// `int` list → `PgVec<i32>` (`list_make_int` image).
fn ints_to_pgvec<'mcx>(mcx: Mcx<'mcx>, ints: &[i32]) -> PgResult<PgVec<'mcx, i32>> {
    let mut v = mcx::vec_with_capacity_in::<i32>(mcx, ints.len())?;
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
    let mut v = mcx::vec_with_capacity_in::<ParseNamespaceColumn>(mcx, cols.len())?;
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
    let mut v = mcx::vec_with_capacity_in::<ParseNamespaceColumn>(mcx, cols.len())?;
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
    let mut v = mcx::vec_with_capacity_in::<ParseNamespaceItem<'mcx>>(mcx, namespace.len())?;
    for ns in namespace.iter() {
        v.push(clone_nsitem(mcx, ns)?);
    }
    Ok(v)
}

/// Borrowed `Expr` view of the transformed func expr nodes for
/// `assign_list_collations`, which mutates the `Expr`s in place.
fn funcexprs_to_expr_vec(funcexprs: &[NodePtr<'_>]) -> PgResult<Vec<Expr>> {
    let mut v = Vec::with_capacity(funcexprs.len());
    for n in funcexprs.iter() {
        match &**n {
            Node::Expr(e) => v.push(e.clone()),
            _ => return Err(elog_error("transformRangeFunction: funcexpr is not a transformed Expr")),
        }
    }
    Ok(v)
}

/// Store collation-assigned `Expr`s back into the `funcexprs` node list.
fn store_back_funcexprs<'mcx>(
    mcx: Mcx<'mcx>,
    funcexprs: &mut [NodePtr<'mcx>],
    exprs: Vec<Expr>,
) -> PgResult<()> {
    debug_assert_eq!(funcexprs.len(), exprs.len());
    for (slot, e) in funcexprs.iter_mut().zip(exprs.into_iter()) {
        *slot = alloc_in(mcx, Node::Expr(e))?;
    }
    Ok(())
}

/// `(Node *) funcexprs` — the func-expr list wrapped as a `T_List` node for
/// `contain_vars_of_level`.
fn funcexprs_as_list_node<'mcx>(
    mcx: Mcx<'mcx>,
    funcexprs: &[NodePtr<'mcx>],
) -> PgResult<Node<'mcx>> {
    let mut v = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, funcexprs.len())?;
    for n in funcexprs.iter() {
        v.push(alloc_in(mcx, (**n).clone_in(mcx)?)?);
    }
    Ok(Node::List(v))
}

