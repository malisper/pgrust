//! Top-level utility-statement entry points (`parse_utilcmd.c`).
//!
//! [`transformCreateStmt`] is GROUNDED: the `ParseState` / `CreateStmtContext`
//! setup, the element dispatch, the not-null propagation, the index-constraint /
//! FK / CHECK postprocessing, and the before/after output assembly all run
//! in-crate over the owned node tree; only the creation-namespace lookup, type
//! validation, and the catalog/relcache leaves cross the outward seams.
//!
//! [`transformIndexStmt`] / [`transformStatsStmt`] (in [`crate::index_stats`])
//! open the target relation by OID through the relcache (an owned
//! [`rel::Relation`] carrier, RAII-closed) and transform the WHERE
//! predicate / index-element / stat expressions in-crate. The creation-namespace
//! lookup ([`range_var_get_and_check_creation_namespace`]) is likewise grounded.
//! [`transformAlterTableStmt`] still routes through the outward seam (its
//! per-subcommand relcache dispatch is not yet reachable).
//! [`transformRuleStmt`] (the inward seam this crate owns) ports the
//! entry point and delegates the relcache OLD/NEW fake-RTE + analyze.c-driven
//! action transform to the outward seam.

use ::mcx::{Mcx, PgBox, PgString, PgVec};

use ::utils_error::ereport;
use ::types_core::Oid;
use ::types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
    NOTICE,
};

use ::nodes::copy_query::Query;
use ::nodes::ddlnodes::{CreateStmt, RuleStmt};
use ::nodes::nodes::{ntag, Node};

use ::small1::{free_parsestate, make_parsestate, parser_errposition};
use ::types_storage::lock::NoLock;

use crate::column::transformColumnDefinition;
use crate::constraint::{transformCheckConstraints, transformTableConstraint};
use crate::core::{CreateStmtContext, NodePtr};
use crate::fk_check_attrs::transformFKConstraints;
use crate::index_constraint::transformIndexConstraints;
use crate::like::{transformOfType, transformTableLikeClause};

const INVALID_OID: Oid = 0;
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `ErrorLocation` for `ereport(...).finish(...)` non-error emits in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("parse_utilcmd.c", 0, funcname)
}

/// `transformCreateStmt` — parse analysis for CREATE TABLE. Returns a list of
/// utility commands to be executed in sequence (the transformed `CreateStmt`,
/// preceded by `cxt.blist` and followed by `cxt.alist` / `save_alist`).
pub fn transformCreateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    // The caller hands us a CreateStmt (or CreateForeignTableStmt). The shared
    // struct is `CreateStmt`; the foreign-table variant carries the same fields.
    let stmt_node = PgBox::into_inner(stmt);
    let stmt_tag = stmt_node.node_tag();
    // For CREATE FOREIGN TABLE the input node is a CreateForeignTableStmt whose
    // `base` is the shared CreateStmt; in C the original node pointer is cast to
    // CreateStmt* and re-appended to `result` with its T_CreateForeignTableStmt
    // tag intact (parse_utilcmd.c:373), so ProcessUtilitySlow takes the foreign
    // branch and DefineRelation is called with RELKIND_FOREIGN_TABLE. We must
    // therefore carry the servername/options forward and re-wrap below.
    let mut foreign_extra: Option<(Option<PgString<'mcx>>, PgVec<'mcx, NodePtr<'mcx>>)> = None;
    let (mut stmt, isforeign, stmt_type): (CreateStmt<'mcx>, bool, &'static str) =
        match stmt_tag {
            ntag::T_CreateStmt => (stmt_node.into_createstmt().unwrap(), false, "CREATE TABLE"),
            ntag::T_CreateForeignTableStmt => {
                let cft = stmt_node.into_createforeigntablestmt().unwrap();
                foreign_extra = Some((cft.servername, cft.options));
                (PgBox::into_inner(cft.base), true, "CREATE FOREIGN TABLE")
            }
            _ => unreachable!("transformCreateStmt: not a CreateStmt node: {}", stmt_tag),
        };

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

    // Look up (and permission-check / lock) the creation namespace. Returns the
    // (mutated) relation node, any preexisting relation of that name, and the
    // namespace name (used to schema-qualify the relation).
    let relation = match stmt.relation.take() {
        Some(rv) => rv,
        None => unreachable!("CreateStmt.relation must be a RangeVar"),
    };
    // C wraps this lookup in
    //   setup_parser_errposition_callback(&pcbstate, pstate, stmt->relation->location)
    // so a sub-error (e.g. the temp/non-temp-schema mismatch raised by
    // RangeVarAdjustRelationPersistence) reports the relation's source position.
    // The ambient callback chain is retired (docs/query-lifecycle-raii.md); attach
    // the location at the propagation site exactly as pcb_error_callback does — tag
    // the error with parser_errposition(pstate, location) as the cursor position,
    // but only when it has none of its own (C: `if (edata->cursorpos == 0)`).
    let rel_location =
        relation.as_rangevar().map_or(-1, |rv| rv.location);
    let (relation, existing_relid, namespace_name) =
        range_var_get_and_check_creation_namespace(mcx, relation).map_err(|mut e| {
            if e.cursor_position().is_none() {
                let pos = parser_errposition(&pstate, rel_location);
                if pos > 0 {
                    e = e.with_cursor_position(pos);
                }
            }
            e
        })?;
    stmt.relation = Some(relation);

    // Pull the (possibly-mutated) relation's schemaname / relpersistence / name.
    let (schemaname_is_none, relpersistence, relname) = match stmt.relation.as_deref().and_then(|n| n.as_rangevar()) {
        Some(rv) => (
            rv.schemaname.is_none(),
            rv.relpersistence,
            rv.relname.as_ref().map_or_else(alloc::string::String::new, |s| s.as_str().into()),
        ),
        None => unreachable!("CreateStmt.relation must be a RangeVar"),
    };

    // IF NOT EXISTS and the relation already exists: bail with a NOTICE.
    if stmt.if_not_exists && existing_relid != INVALID_OID {
        // (checkMembershipInCurrentExtension would run here, behind the namespace
        // lookup above.)
        ereport(NOTICE)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg(alloc::format!("relation \"{relname}\" already exists, skipping"))
            .finish(here("transformCreateStmt"))?;
        return Ok(PgVec::new_in(mcx));
    }

    // If the target name isn't schema-qualified, make it so (unless a local temp
    // table, which is effectively in pg_temp).
    if schemaname_is_none && relpersistence != RELPERSISTENCE_TEMP {
        if let Some(rv) = stmt.relation.as_deref_mut().and_then(|n| n.as_rangevar_mut()) {
            rv.schemaname = namespace_name;
        }
    }

    // Set up CreateStmtContext.
    let ispartitioned = stmt.partspec.is_some();
    let oftype = stmt.ofTypename.is_some();
    let relation_clone = match stmt.relation.as_deref() {
        Some(n) => Some(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
        None => None,
    };
    let inh_relations = {
        let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        for n in stmt.inhRelations.iter() {
            v.push(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
        v
    };
    let mut cxt = CreateStmtContext {
        mcx,
        pstate,
        stmtType: stmt_type,
        relation: relation_clone,
        rel_oid: INVALID_OID,
        inhRelations: inh_relations,
        isforeign,
        isalter: false,
        columns: PgVec::new_in(mcx),
        ckconstraints: PgVec::new_in(mcx),
        nnconstraints: PgVec::new_in(mcx),
        fkconstraints: PgVec::new_in(mcx),
        ixconstraints: PgVec::new_in(mcx),
        likeclauses: PgVec::new_in(mcx),
        blist: PgVec::new_in(mcx),
        alist: PgVec::new_in(mcx),
        pkey: None,
        ispartitioned,
        partbound: match stmt.partbound.as_deref() {
            Some(n) => Some(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
            None => None,
        },
        ofType: oftype,
    };

    // grammar enforces: !stmt->ofTypename || !stmt->inhRelations
    if let Some(of_typename) = stmt.ofTypename.as_deref() {
        let of = ::mcx::alloc_in(mcx, of_typename.clone_in(mcx)?)?;
        transformOfType(&mut cxt, of)?;
    }

    if stmt.partspec.is_some() && !stmt.inhRelations.is_empty() && stmt.partbound.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("cannot create partitioned table as inheritance child")
            .into_error());
    }

    // Run through each primary element, separating column defs from constraints.
    let table_elts = core::mem::replace(&mut stmt.tableElts, PgVec::new_in(mcx));
    for element in table_elts {
        match element.node_tag() {
            ntag::T_ColumnDef => transformColumnDefinition(&mut cxt, element)?,
            ntag::T_Constraint => transformTableConstraint(&mut cxt, element)?,
            ntag::T_TableLikeClause => transformTableLikeClause(&mut cxt, element)?,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "unrecognized node type: {}",
                        element.node_tag()
                    ))
                    .into_error());
            }
        }
    }

    // Transfer anything already in cxt.alist into save_alist.
    let save_alist = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));

    // Before processing index constraints, scan all not-null constraints to
    // propagate the is_not_null flag to each corresponding ColumnDef.
    propagate_notnull(&mut cxt);

    // Postprocess constraints that give rise to index definitions.
    transformIndexConstraints(&mut cxt)?;

    // Re-consideration of LIKE clauses happens after index creation but before
    // foreign keys.
    let likeclauses = core::mem::replace(&mut cxt.likeclauses, PgVec::new_in(mcx));
    cxt.alist.extend(likeclauses);

    // Postprocess foreign-key constraints.
    transformFKConstraints(&mut cxt, true, false)?;

    // Postprocess check constraints (skip validation for new non-foreign tables).
    let skip = !cxt.isforeign;
    transformCheckConstraints(&mut cxt, skip);

    // Output results.
    stmt.tableElts = core::mem::replace(&mut cxt.columns, PgVec::new_in(mcx));
    stmt.constraints = core::mem::replace(&mut cxt.ckconstraints, PgVec::new_in(mcx));
    stmt.nnconstraints = core::mem::replace(&mut cxt.nnconstraints, PgVec::new_in(mcx));

    let mut result = core::mem::replace(&mut cxt.blist, PgVec::new_in(mcx));
    // Re-wrap the transformed CreateStmt as the original node kind so the tag is
    // preserved (C re-appends the same node pointer): a foreign table must stay a
    // CreateForeignTableStmt so ProcessUtilitySlow uses RELKIND_FOREIGN_TABLE and
    // CreateForeignTable runs with the carried servername/options.
    let stmt_out = match foreign_extra {
        Some((servername, options)) => {
            let cft = ::nodes::ddlnodes::CreateForeignTableStmt {
                base: ::mcx::alloc_in(mcx, stmt)?,
                servername,
                options,
            };
            Node::mk_create_foreign_table_stmt(mcx, cft)?
        }
        None => Node::mk_create_stmt(mcx, stmt)?,
    };
    result.push(::mcx::alloc_in(mcx, stmt_out)?);
    let alist = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));
    result.extend(alist);
    result.extend(save_alist);

    Ok(result)
}

/// The not-null-propagation loop from `transformCreateStmt`: for each table-level
/// NOT NULL constraint, set `is_not_null` on the matching `ColumnDef`.
fn propagate_notnull(cxt: &mut CreateStmtContext<'_>) {
    // Collect target column names first (strVal(linitial(nn->keys))).
    let mut colnames: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();
    for nn in cxt.nnconstraints.iter() {
        if let Some(c) = nn.as_constraint() {
            if let Some(k) = c.keys.first() {
                if let Some(s) = k.as_string() {
                    colnames.push(s.sval.as_str().into());
                }
            }
        }
    }

    for colname in colnames {
        for cd in cxt.columns.iter_mut() {
            if let Some(col) = cd.as_columndef_mut() {
                // not our column?
                if col.colname.as_ref().map(PgString::as_str) != Some(colname.as_str()) {
                    continue;
                }
                // Already marked not-null? Nothing to do
                if col.is_not_null {
                    break;
                }
                // Bingo, we're done for this constraint
                col.is_not_null = true;
                break;
            }
        }
    }
}

pub use crate::alter::transformAlterTableStmt;

pub use crate::index_stats::{transformIndexStmt, transformStatsStmt};

/// The `RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock,
/// &existing_relid)` + `get_namespace_name(namespaceid)` pair from
/// `transformCreateStmt`. Looks up (and permission-checks / locks) the creation
/// namespace, finds any preexisting relation of the same name, and resolves the
/// namespace name used to schema-qualify the relation. The (possibly-mutated)
/// `RangeVar` node is threaded in/out. Returns `(relation, existing_relid,
/// namespace_name)`.
fn range_var_get_and_check_creation_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    mut relation: NodePtr<'mcx>,
) -> PgResult<(NodePtr<'mcx>, Oid, Option<PgString<'mcx>>)> {
    let mut existing_relid: Oid = INVALID_OID;

    // The catalog-namespace function operates on the value-typed
    // `::types_tuple::access::RangeVar` (no `'mcx`); bridge the node's
    // `rawnodes::RangeVar` across, then propagate the (possibly temp-promoted)
    // `relpersistence` back onto the node.
    let mut access_rv = match relation.node_tag() {
        ntag::T_RangeVar => to_access_range_var(relation.expect_rangevar()),
        _ => unreachable!(
            "RangeVarGetAndCheckCreationNamespace: not a RangeVar node: {}",
            relation.node_tag()
        ),
    };
    let namespaceid = catalog_namespace::RangeVarGetAndCheckCreationNamespace(
        mcx,
        &mut access_rv,
        NoLock,
        Some(&mut existing_relid),
    )?;
    if let Some(rv) = relation.as_rangevar_mut() {
        rv.relpersistence = access_rv.relpersistence as i8;
    }

    let namespace_name = lsyscache::namespace_range_index_pubsub::get_namespace_name(mcx, namespaceid)?;
    Ok((relation, existing_relid, namespace_name))
}

/// Bridge a node `rawnodes::RangeVar` to the value-typed
/// `::types_tuple::access::RangeVar` the catalog-namespace API consumes.
fn to_access_range_var(rv: &::nodes::rawnodes::RangeVar<'_>) -> ::types_tuple::access::RangeVar {
    ::types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().into()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().into()),
        relname: rv.relname.as_ref().map_or_else(alloc::string::String::new, |s| s.as_str().into()),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `transformRuleStmt` — parse analysis for CREATE RULE (parse_utilcmd.c).
///
/// Opens the event relation under `AccessExclusiveLock`, sets up the OLD/NEW
/// pseudo-relation range-table entries (OLD always varno 1, NEW varno 2),
/// transforms the WHERE qual, and runs each action statement through
/// analyze.c's [`transformStmt`], validating the OLD/NEW usage per event type.
/// Returns `(actions, where_clause)`.
pub fn transformRuleStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RuleStmt<'_>,
    query_string: &str,
) -> PgResult<(PgVec<'mcx, Query<'mcx>>, Option<Node<'mcx>>)> {
    use ::nodes::nodes::CmdType;
    use ::nodes::parsestmt::ParseExprKind::EXPR_KIND_WHERE;
    use ::types_storage::lock::AccessShareLock;
    use ::types_tuple::access::{RangeVar as AccessRangeVar, RELKIND_MATVIEW};

    let stmt = stmt.clone_in(mcx)?;

    // PRS2_OLD_VARNO / PRS2_NEW_VARNO (primnodes.h).
    const PRS2_OLD_VARNO: i32 = 1;
    const PRS2_NEW_VARNO: i32 = 2;

    // To avoid deadlock, the first thing we do is grab AccessExclusiveLock on
    // the target relation. This will be needed by DefineQueryRewrite().
    let rel_rv: AccessRangeVar = match stmt.relation.as_ref().and_then(|n| n.as_rangevar()) {
        Some(rv) => to_access_range_var(rv),
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("CREATE RULE: missing or invalid event relation")
                .into_error())
        }
    };
    let rel = table::table_openrv(
        mcx,
        &rel_rv,
        ::types_storage::lock::AccessExclusiveLock,
    )?;

    if rel.rd_rel.relkind == RELKIND_MATVIEW {
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("rules on materialized views are not supported")
            .into_error());
    }

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

    // NOTE: 'OLD' must always have a varno equal to 1 and 'NEW' equal to 2.
    let old_alias = make_old_new_alias(mcx, "old")?;
    let new_alias = make_old_new_alias(mcx, "new")?;
    let oldnsitem = parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        &rel,
        AccessShareLock,
        Some(old_alias),
        false,
        false,
    )?;
    let newnsitem = parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        &rel,
        AccessShareLock,
        Some(new_alias),
        false,
        false,
    )?;

    // They must be in the namespace too for lookup purposes, but only add the
    // one(s) relevant for the current kind of rule. Not added to the joinlist.
    match stmt.event {
        CmdType::CMD_SELECT => {
            parser_relation::addNSItemToQuery(
                mcx, &mut pstate, oldnsitem, false, true, true,
            )?;
        }
        CmdType::CMD_UPDATE => {
            parser_relation::addNSItemToQuery(
                mcx, &mut pstate, oldnsitem, false, true, true,
            )?;
            parser_relation::addNSItemToQuery(
                mcx, &mut pstate, newnsitem, false, true, true,
            )?;
        }
        CmdType::CMD_INSERT => {
            parser_relation::addNSItemToQuery(
                mcx, &mut pstate, newnsitem, false, true, true,
            )?;
        }
        CmdType::CMD_DELETE => {
            parser_relation::addNSItemToQuery(
                mcx, &mut pstate, oldnsitem, false, true, true,
            )?;
        }
        ev => {
            return Err(ereport(ERROR)
                .errmsg(&alloc::format!("unrecognized event type: {}", ev as i32))
                .into_error())
        }
    }

    // Take care of the where clause.
    let where_clause_in: Option<Node<'mcx>> = stmt
        .where_clause
        .as_ref()
        .map(|n| n.clone_in(mcx))
        .transpose()?;
    let mut where_clause: Option<Node<'mcx>> = {
        let e = clause::transformWhereClause(
            mcx,
            &mut pstate,
            where_clause_in,
            EXPR_KIND_WHERE,
            "WHERE",
        )?;
        // Bring the parser-arena `'static` qual into `mcx` for the in-place
        // collation pass and the `'mcx` Node wrap (`Expr` is invariant).
        let mut e: Option<::nodes::primnodes::Expr<'mcx>> = match e {
            Some(expr) => Some(expr.clone_in(mcx)?),
            None => None,
        };
        // We have to fix its collations too.
        if let Some(expr) = e.as_mut() {
            parse_collate::assign_expr_collations(Some(&pstate), expr)?;
        }
        match e {
            Some(expr) => Some(Node::mk_expr(mcx, expr)?),
            None => None,
        }
    };

    // This is probably dead code without add_missing_from.
    if pstate.p_rtable.len() != 2 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("rule WHERE condition cannot contain references to other relations")
            .into_error());
    }

    let actions: PgVec<'mcx, Query<'mcx>>;

    if stmt.actions.is_empty() {
        // 'instead nothing' rules with a qualification need a query rangetable
        // so the rewrite handler can add the negated rule qualification to the
        // original query. We create a query with command type CMD_NOTHING.
        let mut nothing_qry = Query::new(mcx);
        nothing_qry.commandType = CmdType::CMD_NOTHING;
        nothing_qry.rtable = core::mem::replace(&mut pstate.p_rtable, PgVec::new_in(mcx));
        nothing_qry.rteperminfos =
            core::mem::replace(&mut pstate.p_rteperminfos, PgVec::new_in(mcx));
        nothing_qry.jointree = Some(PgBox::new_in(
            ::nodes::rawnodes::FromExpr {
                fromlist: PgVec::new_in(mcx),
                quals: None,
            },
            mcx,
        ));
        let mut v = PgVec::new_in(mcx);
        v.push(nothing_qry);
        actions = v;
    } else {
        let mut newactions: PgVec<'mcx, Query<'mcx>> = PgVec::new_in(mcx);

        // Transform each statement, like parse_sub_analyze().
        for action in stmt.actions.iter() {
            let mut sub_pstate = make_parsestate(mcx, None)?;
            // Outer ParseState isn't parent of inner: pass the text by hand.
            sub_pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

            // Set up OLD/NEW in the rtable for this statement, in relnamespace
            // only (not varnamespace); they aren't referred to by unqualified
            // field names nor "*" in rule actions.
            let s_old_alias = make_old_new_alias(mcx, "old")?;
            let s_new_alias = make_old_new_alias(mcx, "new")?;
            let s_oldnsitem = parser_relation::addRangeTableEntryForRelation(
                mcx,
                &mut sub_pstate,
                &rel,
                AccessShareLock,
                Some(s_old_alias),
                false,
                false,
            )?;
            let s_old_rtindex = s_oldnsitem.p_rtindex;
            let s_newnsitem = parser_relation::addRangeTableEntryForRelation(
                mcx,
                &mut sub_pstate,
                &rel,
                AccessShareLock,
                Some(s_new_alias),
                false,
                false,
            )?;
            parser_relation::addNSItemToQuery(
                mcx,
                &mut sub_pstate,
                s_oldnsitem,
                false,
                true,
                false,
            )?;
            parser_relation::addNSItemToQuery(
                mcx,
                &mut sub_pstate,
                s_newnsitem,
                false,
                true,
                false,
            )?;

            // Transform the rule action statement.
            let action_node = action.clone_in(mcx)?;
            let mut top_subqry =
                parser_analyze::transformStmt(mcx, &mut sub_pstate, &action_node)?;

            // We cannot support utility-statement actions (eg NOTIFY) with a
            // nonempty rule WHERE condition.
            if top_subqry.commandType == CmdType::CMD_UTILITY && where_clause.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(
                        "rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions",
                    )
                    .into_error());
            }

            // If the action is INSERT...SELECT, OLD/NEW have been pushed down
            // into the SELECT, and that's what we need to look at. We resolve
            // the sub-query index so we can also mutate its jointree below.
            let sub_idx =
                rewrite_core::insert_select::getInsertSelectQueryIndex(&top_subqry)?;

            // Reject conditional set-ops up front.
            {
                let sub_qry: &Query = match sub_idx {
                    Some(i) => sub_query_at(&top_subqry, i)?,
                    None => &top_subqry,
                };
                if sub_qry.setOperations.is_some() && where_clause.is_some() {
                    return Err(ereport(ERROR)
                        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
                        .into_error());
                }
            }

            // Validate action's use of OLD/NEW, qual too.
            let (has_old, has_new) = {
                let sub_qry: &Query = match sub_idx {
                    Some(i) => sub_query_at(&top_subqry, i)?,
                    None => &top_subqry,
                };
                let sub_qry_node = Node::mk_query(mcx, sub_qry.clone_in(mcx)?)?;
                let has_old = rewrite_core::walkers::rangeTableEntry_used(
                    &sub_qry_node,
                    PRS2_OLD_VARNO,
                    0,
                ) || where_clause.as_ref().is_some_and(|w| {
                    rewrite_core::walkers::rangeTableEntry_used(w, PRS2_OLD_VARNO, 0)
                });
                let has_new = rewrite_core::walkers::rangeTableEntry_used(
                    &sub_qry_node,
                    PRS2_NEW_VARNO,
                    0,
                ) || where_clause.as_ref().is_some_and(|w| {
                    rewrite_core::walkers::rangeTableEntry_used(w, PRS2_NEW_VARNO, 0)
                });
                (has_old, has_new)
            };

            match stmt.event {
                CmdType::CMD_SELECT => {
                    if has_old {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("ON SELECT rule cannot use OLD")
                            .into_error());
                    }
                    if has_new {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("ON SELECT rule cannot use NEW")
                            .into_error());
                    }
                }
                CmdType::CMD_UPDATE => { /* both are OK */ }
                CmdType::CMD_INSERT => {
                    if has_old {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("ON INSERT rule cannot use OLD")
                            .into_error());
                    }
                }
                CmdType::CMD_DELETE => {
                    if has_new {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("ON DELETE rule cannot use NEW")
                            .into_error());
                    }
                }
                ev => {
                    return Err(ereport(ERROR)
                        .errmsg(&alloc::format!("unrecognized event type: {}", ev as i32))
                        .into_error())
                }
            }

            // OLD/NEW are not allowed in WITH queries (they would amount to
            // outer references for the WITH, which we disallow). Check both the
            // top_subqry and sub_qry CTE lists.
            {
                // C wraps each cteList (a List*) as a Node* for the walk; here
                // we walk the list elements directly, which is equivalent
                // (query_or_expression_tree_walker over a List visits members).
                let cte_used = |varno: i32| -> bool {
                    let top_hit = top_subqry.cteList.iter().any(|c| {
                        rewrite_core::walkers::rangeTableEntry_used(c, varno, 0)
                    });
                    if top_hit {
                        return true;
                    }
                    let sub_qry: &Query = match sub_idx {
                        Some(i) => match sub_query_at(&top_subqry, i) {
                            Ok(q) => q,
                            Err(_) => return false,
                        },
                        None => &top_subqry,
                    };
                    sub_qry.cteList.iter().any(|c| {
                        rewrite_core::walkers::rangeTableEntry_used(c, varno, 0)
                    })
                };
                if cte_used(PRS2_OLD_VARNO) {
                    return Err(ereport(ERROR)
                        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot refer to OLD within WITH query")
                        .into_error());
                }
                if cte_used(PRS2_NEW_VARNO) {
                    return Err(ereport(ERROR)
                        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot refer to NEW within WITH query")
                        .into_error());
                }
            }

            // For efficiency's sake, add OLD to the rule action's jointree only
            // if it was actually referenced. For INSERT, NEW is not a relation;
            // for UPDATE NEW is another reference to OLD.
            if has_old || (has_new && stmt.event == CmdType::CMD_UPDATE) {
                // Mutate the (possibly pushed-down) sub-query's jointree.
                let sub_qry_mut: &mut Query = match sub_idx {
                    Some(i) => sub_query_at_mut(&mut top_subqry, i)?,
                    None => &mut top_subqry,
                };
                if sub_qry_mut.setOperations.is_some() {
                    // Can't-happen case (rejected above), but guard anyway.
                    return Err(ereport(ERROR)
                        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")
                        .into_error());
                }
                let rtr = Node::mk_range_tbl_ref(
                    mcx,
                    ::nodes::rawnodes::RangeTblRef {
                        rtindex: s_old_rtindex,
                    },
                )?;
                if let Some(jt) = sub_qry_mut.jointree.as_mut() {
                    jt.fromlist.push(::mcx::alloc_in(mcx, rtr)?);
                }
            }

            newactions.push(top_subqry);
            free_parsestate(sub_pstate)?;
        }

        actions = newactions;
    }

    // Silence unused-mut when no actions branch ran.
    let _ = &mut where_clause;

    free_parsestate(pstate)?;

    // Close relation, but keep the exclusive lock (RAII drop releases the
    // relcache reference; the lock is retained for DefineQueryRewrite).
    table::table_close(rel, NoLock)?;

    Ok((actions, where_clause))
}

/// `makeAlias("old"/"new", NIL)` — build a bare relation alias.
fn make_old_new_alias<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
) -> PgResult<::nodes::rawnodes::Alias<'mcx>> {
    Ok(::nodes::rawnodes::Alias {
        aliasname: Some(PgString::from_str_in(name, mcx)?),
        colnames: PgVec::new_in(mcx),
    })
}

/// Resolve the `Query` at sub-query index `i` (a `getInsertSelectQuery`
/// position): the SELECT pushed into an INSERT's `RTE_SUBQUERY` range-table
/// entry. `&Query` view.
fn sub_query_at<'a, 'mcx>(top: &'a Query<'mcx>, rtindex: usize) -> PgResult<&'a Query<'mcx>> {
    top.rtable
        .get(rtindex - 1)
        .and_then(|rte| rte.subquery.as_deref())
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg("transformRuleStmt: INSERT...SELECT subquery RTE missing")
                .into_error()
        })
}

/// Mutable counterpart of [`sub_query_at`].
fn sub_query_at_mut<'a, 'mcx>(
    top: &'a mut Query<'mcx>,
    rtindex: usize,
) -> PgResult<&'a mut Query<'mcx>> {
    top.rtable
        .get_mut(rtindex - 1)
        .and_then(|rte| rte.subquery.as_deref_mut())
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg("transformRuleStmt: INSERT...SELECT subquery RTE missing")
                .into_error()
        })
}
