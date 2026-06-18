#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `backend/rewrite/rewriteDefine.c` — `CREATE RULE` and friends.
//!
//! Faithful port of rewriteDefine.c (PostgreSQL 18.3):
//!
//!   * [`DefineRule`] — the `CREATE RULE` command driver (parse-analyse +
//!     lock + [`DefineQueryRewrite`]);
//!   * [`DefineQueryRewrite`] — create a rule (the validation gauntlet +
//!     `InsertRule` + `SetRelationRuleStatus`);
//!   * [`setRuleCheckAsUser`] — stamp the `checkAsUser` on every
//!     `RTEPermissionInfo` of a query/expression tree;
//!   * [`EnableDisableRule`] — `ALTER … ENABLE/DISABLE RULE`;
//!   * [`RenameRewriteRule`] — `ALTER … RENAME RULE`.
//!
//! `InsertRule`'s catalog mechanics (`heap_form_tuple`/`heap_modify_tuple` +
//! `GetNewOidWithIndex` + `CatalogTupleInsert`/`Update`) cross the typed
//! `backend-catalog-indexing` pg_rewrite seams; the RULERELNAME / RELOID
//! syscache probes cross the typed `backend-utils-cache-syscache` projections.
//! Parse analysis (`transformRuleStmt`) and the relhasrules /
//! duplicate-name helpers (`SetRelationRuleStatus`/`IsDefinedRewriteRule`,
//! rewriteSupport.c) are still-unported owners and cross their `-seams` crates
//! (loud panic until they land). `getInsertSelectQuery` is the rewriteManip.c
//! owner's function, called directly via `backend-rewrite-core`.

use mcx::{Mcx, PgVec};
use types_acl::acl::ACLCHECK_NOT_OWNER;
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
};
use types_catalog::pg_rewrite::{RewriteRelationId, ViewSelectRuleName};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsenodes::RTEKind;
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use types_tuple::access::{
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};

use backend_rewrite_core::getInsertSelectQuery;

/// `RelationRelationId` — `pg_class` (`pg_class_d.h`).
const RelationRelationId: Oid = 1259;
/// `CMD_SELECT` cast to its `int` value `1`; `ev_type = evtype + '0'`.
const NAMEDATALEN: usize = 64;

/// Faithful field copy of an owned-tree `RangeVar` node into the access-layer
/// `RangeVar` the namespace core consumes (mirrors `to_access_range_var` in the
/// sibling command crates).
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `errdetail_relkind_not_supported(relkind)` attached to a `PgError`.
fn attach_relkind_detail(err: PgError, relkind: u8) -> PgResult<PgError> {
    let detail = backend_catalog_pg_class::errdetail_relkind_not_supported(relkind)?;
    Ok(err.with_detail(detail))
}

/* ===========================================================================
 * InsertRule (rewriteDefine.c:51)
 * ========================================================================= */

/// `InsertRule` — build and insert (or replace) the `pg_rewrite` tuple, then
/// install the dependencies. Returns the rule's OID.
fn InsertRule<'mcx>(
    mcx: Mcx<'mcx>,
    rulname: &str,
    evtype: CmdType,
    eventrel_oid: Oid,
    evinstead: bool,
    event_qual: Option<&Node<'mcx>>,
    action: &[Query<'mcx>],
    replace: bool,
) -> PgResult<Oid> {
    // char *evqual = nodeToString(event_qual);
    // nodeToString(NULL) renders a NULL pointer as "<>".
    let evqual: String = match event_qual {
        Some(node) => backend_nodes_outfuncs::nodeToString(mcx, node)?.as_str().to_string(),
        None => "<>".to_string(),
    };
    // char *actiontree = nodeToString((Node *) action);
    let action_list_node = action_as_list_node(mcx, action)?;
    let actiontree = backend_nodes_outfuncs::nodeToString(mcx, &action_list_node)?;

    // pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);
    let pg_rewrite_desc =
        backend_access_table_table::table_open(mcx, RewriteRelationId, RowExclusiveLock)?;

    let rewriteObjectId: Oid;
    let is_update: bool;

    // oldtup = SearchSysCache2(RULERELNAME, eventrel_oid, rulname);
    let oldtup = backend_utils_cache_syscache_seams::rule_tuple_by_relname::call(
        mcx,
        eventrel_oid,
        rulname,
    )?;

    if let Some((oldtuple, _oldform)) = oldtup {
        // if (!replace) ereport(ERROR, duplicate rule);
        if !replace {
            let relname = get_rel_name_or_empty(mcx, eventrel_oid)?;
            return Err(PgError::new(
                ERROR,
                format!("rule \"{rulname}\" for relation \"{relname}\" already exists"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
        }

        // replaces[] true for ev_type / is_instead / ev_qual / ev_action;
        // tup = heap_modify_tuple(oldtup, ...); CatalogTupleUpdate(rel, &tup->t_self, tup);
        // rewriteObjectId = ((Form_pg_rewrite) GETSTRUCT(tup))->oid;
        rewriteObjectId = backend_catalog_indexing_seams::catalog_tuple_update_pg_rewrite::call(
            mcx,
            &pg_rewrite_desc,
            &oldtuple,
            cmdtype_ev_type(evtype),
            evinstead,
            evqual.as_str(),
            actiontree.as_str(),
        )?;
        is_update = true;
    } else {
        // rewriteObjectId = GetNewOidWithIndex(...); tup = heap_form_tuple(...);
        // CatalogTupleInsert(rel, tup);
        rewriteObjectId = backend_catalog_indexing_seams::catalog_tuple_insert_pg_rewrite::call(
            mcx,
            &pg_rewrite_desc,
            rulname,
            eventrel_oid,
            cmdtype_ev_type(evtype),
            evinstead,
            evqual.as_str(),
            actiontree.as_str(),
        )?;
        is_update = false;
    }

    // heap_freetuple(tup);  (the formed tuple drops inside the seam)

    /* If replacing, get rid of old dependencies and make new ones */
    if is_update {
        // deleteDependencyRecordsFor(RewriteRelationId, rewriteObjectId, false);
        backend_catalog_pg_depend::deleteDependencyRecordsFor(
            RewriteRelationId,
            rewriteObjectId,
            false,
        )?;
    }

    /*
     * Install dependency on rule's relation to ensure it will go away on
     * relation deletion.  If the rule is ON SELECT, make the dependency
     * implicit (DEPENDENCY_INTERNAL); other kinds of rules can be AUTO.
     */
    let myself = ObjectAddress {
        classId: RewriteRelationId,
        objectId: rewriteObjectId,
        objectSubId: 0,
    };
    let referenced = ObjectAddress {
        classId: RelationRelationId,
        objectId: eventrel_oid,
        objectSubId: 0,
    };
    let rel_behavior = if evtype == CmdType::CMD_SELECT {
        DEPENDENCY_INTERNAL
    } else {
        DEPENDENCY_AUTO
    };
    backend_catalog_pg_depend::recordDependencyOn(mcx, &myself, &referenced, rel_behavior)?;

    /*
     * Also install dependencies on objects referenced in action and qual.
     */
    // recordDependencyOnExpr(&myself, (Node *) action, NIL, DEPENDENCY_NORMAL);
    backend_catalog_dependency::recordDependencyOnExpr(
        &myself,
        &action_list_node,
        &[],
        DEPENDENCY_NORMAL,
    )?;

    if let Some(event_qual) = event_qual {
        // Query *qry = linitial_node(Query, action);
        // qry = getInsertSelectQuery(qry, NULL);
        // recordDependencyOnExpr(&myself, event_qual, qry->rtable, DEPENDENCY_NORMAL);
        let qry = action
            .first()
            .ok_or_else(|| PgError::error("InsertRule: empty action list with event qual"))?;
        let qry = getInsertSelectQuery(qry)?;
        backend_catalog_dependency::recordDependencyOnExpr(
            &myself,
            event_qual,
            &qry.rtable,
            DEPENDENCY_NORMAL,
        )?;
    }

    /* Post creation hook for new rule */
    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        RewriteRelationId,
        rewriteObjectId,
        0,
    )?;

    // table_close(pg_rewrite_desc, RowExclusiveLock);
    pg_rewrite_desc.close(RowExclusiveLock)?;

    Ok(rewriteObjectId)
}

/// `ev_type = CmdType + '0'` — the one-character event-type code stored in
/// `pg_rewrite.ev_type`.
fn cmdtype_ev_type(evtype: CmdType) -> u8 {
    (evtype as i32 as u8).wrapping_add(b'0')
}

/// Wrap an action list (`List *` of `Query`) into a `Node::List` of
/// `Node::Query` for `nodeToString` / `recordDependencyOnExpr`, mirroring C's
/// `(Node *) action` cast over the existing list pointer.
fn action_as_list_node<'mcx>(mcx: Mcx<'mcx>, action: &[Query<'mcx>]) -> PgResult<Node<'mcx>> {
    let mut items: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, action.len())?;
    for q in action.iter() {
        items.push(mcx::alloc_in(mcx, Node::Query(q.clone_in(mcx)?))?);
    }
    Ok(Node::List(items))
}

fn get_rel_name_or_empty<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    match backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)? {
        Some(s) => Ok(s.as_str().to_string()),
        None => Ok(String::new()),
    }
}

/* ===========================================================================
 * DefineRule (rewriteDefine.c:189)
 * ========================================================================= */

/// `DefineRule(RuleStmt *stmt, const char *queryString)` — execute `CREATE
/// RULE`.
pub fn DefineRule<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::ddlnodes::RuleStmt<'_>,
    query_string: &str,
) -> PgResult<ObjectAddress> {
    /* Parse analysis. */
    // transformRuleStmt(stmt, queryString, &actions, &whereClause);
    let (actions, where_clause) =
        backend_parser_parse_utilcmd_seams::transformRuleStmt::call(mcx, stmt, query_string)?;

    /*
     * Find and lock the relation.  Lock level should match
     * DefineQueryRewrite.
     */
    // relId = RangeVarGetRelid(stmt->relation, AccessExclusiveLock, false);
    let relation_node = stmt
        .relation
        .as_ref()
        .ok_or_else(|| PgError::error("DefineRule: RuleStmt has no relation"))?;
    let range_var = match &**relation_node {
        Node::RangeVar(rv) => to_access_range_var(rv),
        _ => return Err(PgError::error("DefineRule: RuleStmt relation is not a RangeVar")),
    };
    let rel_id = backend_catalog_namespace::RangeVarGetRelid(
        mcx,
        &range_var,
        AccessExclusiveLock,
        false,
    )?;

    /* ... and execute */
    DefineQueryRewrite(
        mcx,
        stmt.rulename.as_deref().unwrap_or(""),
        rel_id,
        where_clause.as_ref(),
        stmt.event,
        stmt.instead,
        stmt.replace,
        &actions,
    )
}

/* ===========================================================================
 * DefineQueryRewrite (rewriteDefine.c:223)
 * ========================================================================= */

/// `DefineQueryRewrite` — create a rule from an already parse-analysed action +
/// qual.
pub fn DefineQueryRewrite<'mcx>(
    mcx: Mcx<'mcx>,
    rulename: &str,
    event_relid: Oid,
    event_qual: Option<&Node<'mcx>>,
    event_type: CmdType,
    is_instead: bool,
    replace: bool,
    action: &[Query<'mcx>],
) -> PgResult<ObjectAddress> {
    let mut rulename = rulename;
    let rename_buf;

    /*
     * If we are installing an ON SELECT rule, we had better grab
     * AccessExclusiveLock.  This lock level matches DefineRule.
     */
    let event_relation =
        backend_access_table_table::table_open(mcx, event_relid, AccessExclusiveLock)?;

    /*
     * Verify relation is of a type that rules can sensibly be applied to.
     */
    let relkind = event_relation.rd_rel.relkind;
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_VIEW
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let err = PgError::new(
            ERROR,
            format!(
                "relation \"{}\" cannot have rules",
                event_relation.name()
            ),
        )
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
        return Err(attach_relkind_detail(err, relkind)?);
    }

    // if (!allowSystemTableMods && IsSystemRelation(event_relation)) ereport(ERROR, ...);
    if !backend_utils_init_small::globals::allowSystemTableMods()
        && backend_catalog_catalog::IsSystemRelation(&event_relation)
    {
        return Err(PgError::new(
            ERROR,
            format!(
                "permission denied: \"{}\" is a system catalog",
                event_relation.name()
            ),
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    /* Check user has permission to apply rules to this relation. */
    // if (!object_ownercheck(RelationRelationId, event_relid, GetUserId()))
    //   aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(relkind), relname);
    let user_id = backend_utils_init_miscinit::GetUserId();
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        RelationRelationId,
        event_relid,
        user_id,
    )? {
        let objtype = backend_catalog_objectaddress::resolve::get_relkind_objtype(relkind);
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objtype,
            Some(event_relation.name().to_string()),
        )?;
    }

    /* No rule actions that modify OLD or NEW */
    for query in action.iter() {
        if query.resultRelation == 0 {
            continue;
        }
        /* Don't be fooled by INSERT/SELECT */
        if !core::ptr::eq(query, getInsertSelectQuery(query)?) {
            continue;
        }
        if query.resultRelation == PRS2_OLD_VARNO {
            return Err(PgError::new(ERROR, "rule actions on OLD are not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint("Use views or triggers instead."));
        }
        if query.resultRelation == PRS2_NEW_VARNO {
            return Err(PgError::new(ERROR, "rule actions on NEW are not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint("Use triggers instead."));
        }
    }

    if event_type == CmdType::CMD_SELECT {
        /* Rules ON SELECT are restricted to view definitions */
        if relkind != RELKIND_VIEW && relkind != RELKIND_MATVIEW {
            let err = PgError::new(
                ERROR,
                format!(
                    "relation \"{}\" cannot have ON SELECT rules",
                    event_relation.name()
                ),
            )
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
            return Err(attach_relkind_detail(err, relkind)?);
        }

        /* ... there cannot be INSTEAD NOTHING, ... */
        if action.is_empty() {
            return Err(PgError::new(
                ERROR,
                "INSTEAD NOTHING rules on SELECT are not implemented",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_hint("Use views instead."));
        }

        /* ... there cannot be multiple actions, ... */
        if action.len() > 1 {
            return Err(PgError::new(
                ERROR,
                "multiple actions for rules on SELECT are not implemented",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        /* ... the one action must be a SELECT, ... */
        let query = &action[0];
        if !is_instead || query.commandType != CmdType::CMD_SELECT {
            return Err(PgError::new(
                ERROR,
                "rules on SELECT must have action INSTEAD SELECT",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        /* ... it cannot contain data-modifying WITH ... */
        if query.hasModifyingCTE {
            return Err(PgError::new(
                ERROR,
                "rules on SELECT must not contain data-modifying statements in WITH",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        /* ... there can be no rule qual, ... */
        if event_qual.is_some() {
            return Err(PgError::new(
                ERROR,
                "event qualifications are not implemented for rules on SELECT",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        /* ... the targetlist of the SELECT action must exactly match the
         * event relation, ... */
        let require_name_match = relkind != RELKIND_MATVIEW;
        checkRuleResultList(
            mcx,
            &query.targetList,
            &event_relation.rd_att,
            true,
            require_name_match,
        )?;

        /* ... there must not be another ON SELECT rule already ...
         *
         * C: `if (!replace && event_relation->rd_rules != NULL) { for each rule:
         *      if rule->event == CMD_SELECT ereport("is already a view"); }`
         *
         * `rd_rules` is the relcache-built rule lock (relcache.c
         * `RelationBuildRuleLock`). Across this repo the relcache rule-lock
         * builder is an unported stub (see
         * backend-utils-cache-relcache::derived::RelationBuildRuleLock, which is
         * a `rule_lock_seam` placeholder), so a relcache entry never carries a
         * rule lock: `rd_rules` is always NULL. C's guard is exactly
         * `rd_rules != NULL`, so with no rule lock the loop is correctly not
         * entered — this matches C behaviour for the NULL case, NOT a silent
         * skip of a populated set. When the relcache rule-lock builder lands, the
         * loop over `rd_rules->rules[]` rejecting a pre-existing CMD_SELECT rule
         * (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: "%s is already a view")
         * lands with it. */
        let _ = (replace, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);

        /* ... and finally the rule must be named _RETURN. */
        if rulename != ViewSelectRuleName {
            // Backwards-compat: accept "_RETviewname" and silently rename.
            let relname = event_relation.name();
            let ok_compat = rulename.len() >= 4
                && &rulename.as_bytes()[..4] == b"_RET"
                && {
                    let limit = NAMEDATALEN - 4 - 4;
                    let suffix = &rulename[4..];
                    let relname_bytes = relname.as_bytes();
                    let n = suffix.len().min(limit);
                    let m = relname_bytes.len().min(limit);
                    suffix.as_bytes()[..n] == relname_bytes[..m] && n == m
                };
            if !ok_compat {
                let err = PgError::new(
                    ERROR,
                    format!(
                        "view rule for \"{}\" must be named \"{}\"",
                        relname, ViewSelectRuleName
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION);
                return Err(err);
            }
            // rulename = pstrdup(ViewSelectRuleName);
            rename_buf = ViewSelectRuleName.to_string();
            rulename = rename_buf.as_str();
        }
    } else {
        /*
         * For non-SELECT rules, a RETURNING list can appear in at most one of
         * the actions, and not at all in conditional/non-INSTEAD rules.
         */
        let mut have_returning = false;
        for query in action.iter() {
            if query.returningList.is_empty() {
                continue;
            }
            if have_returning {
                return Err(PgError::new(
                    ERROR,
                    "cannot have multiple RETURNING lists in a rule",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            have_returning = true;
            if event_qual.is_some() {
                return Err(PgError::new(
                    ERROR,
                    "RETURNING lists are not supported in conditional rules",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            if !is_instead {
                return Err(PgError::new(
                    ERROR,
                    "RETURNING lists are not supported in non-INSTEAD rules",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            checkRuleResultList(
                mcx,
                &query.returningList,
                &event_relation.rd_att,
                false,
                false,
            )?;
        }

        /* And finally, a non-ON-SELECT rule must *not* be named _RETURN. */
        if rulename == ViewSelectRuleName {
            return Err(PgError::new(
                ERROR,
                format!(
                    "non-view rule for \"{}\" must not be named \"{}\"",
                    event_relation.name(),
                    ViewSelectRuleName
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
        }
    }

    /*
     * This rule is allowed - prepare to install it.
     */
    let mut rule_id = InvalidOid;

    /* discard rule if it's null action and not INSTEAD; it's a no-op */
    if !action.is_empty() || is_instead {
        rule_id = InsertRule(
            mcx,
            rulename,
            event_type,
            event_relid,
            is_instead,
            event_qual,
            action,
            replace,
        )?;

        /*
         * Set pg_class 'relhasrules' field true for event relation.  Side
         * effect: an SI notice forces all backends to update relcache.
         */
        backend_rewrite_rewritesupport_seams::SetRelationRuleStatus::call(event_relid, true)?;
    }

    let address = ObjectAddress {
        classId: RewriteRelationId,
        objectId: rule_id,
        objectSubId: 0,
    };

    /* Close rel, but keep lock till commit... */
    event_relation.close(NoLock)?;

    Ok(address)
}

/// `PRS2_OLD_VARNO` / `PRS2_NEW_VARNO` (primnodes.h).
const PRS2_OLD_VARNO: i32 = 1;
const PRS2_NEW_VARNO: i32 = 2;

/* ===========================================================================
 * checkRuleResultList (rewriteDefine.c:505)
 * ========================================================================= */

/// `checkRuleResultList` — verify `targetList` produces output compatible with
/// `resultDesc`.
fn checkRuleResultList<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &[types_nodes::primnodes::TargetEntry<'_>],
    result_desc: &types_tuple::heaptuple::TupleDescData<'_>,
    is_select: bool,
    require_column_name_match: bool,
) -> PgResult<()> {
    /* Only a SELECT may require a column name match. */
    debug_assert!(is_select || !require_column_name_match);

    let natts = result_desc.natts;
    let mut i: i32 = 0;
    for tle in target_list.iter() {
        /* resjunk entries may be ignored */
        if tle.resjunk {
            continue;
        }
        i += 1;
        if i > natts {
            let msg = if is_select {
                "SELECT rule's target list has too many entries"
            } else {
                "RETURNING list has too many entries"
            };
            return Err(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
        }

        let attr = result_desc.attr((i - 1) as usize);
        let attname_bytes = attr.attname.name_str();
        let attname_len = attname_bytes
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(attname_bytes.len());
        let attname = String::from_utf8_lossy(&attname_bytes[..attname_len]).to_string();

        /* Disallow dropped columns in the relation. */
        if attr.attisdropped {
            let msg = if is_select {
                "cannot convert relation containing dropped columns to view"
            } else {
                "cannot create a RETURNING list for a relation containing dropped columns"
            };
            return Err(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        /* Check name match if required. */
        if require_column_name_match {
            let resname = tle.resname.as_deref().unwrap_or("");
            if resname != attname {
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "SELECT rule's target entry {i} has different column name from column \"{attname}\""
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                .with_detail(format!("SELECT target entry is named \"{resname}\".")));
            }
        }

        /* Check type match. */
        let tletypid = backend_nodes_core::nodefuncs::expr_type(tle.expr.as_deref())?;
        if attr.atttypid != tletypid {
            let msg = if is_select {
                format!(
                    "SELECT rule's target entry {i} has different type from column \"{attname}\""
                )
            } else {
                format!("RETURNING list's entry {i} has different type from column \"{attname}\"")
            };
            let tle_ty = backend_utils_adt_format_type::format_type_be_owned(tletypid)?;
            let col_ty = backend_utils_adt_format_type::format_type_be_owned(attr.atttypid)?;
            let detail = if is_select {
                format!("SELECT target entry has type {tle_ty}, but column has type {col_ty}.")
            } else {
                format!("RETURNING list entry has type {tle_ty}, but column has type {col_ty}.")
            };
            return Err(PgError::new(ERROR, msg)
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                .with_detail(detail));
        }

        /*
         * Allow typmods to be different only if one of them is -1.
         */
        let tletypmod = backend_nodes_core::nodefuncs::expr_typmod(tle.expr.as_deref())?;
        if attr.atttypmod != tletypmod && attr.atttypmod != -1 && tletypmod != -1 {
            let msg = if is_select {
                format!(
                    "SELECT rule's target entry {i} has different size from column \"{attname}\""
                )
            } else {
                format!("RETURNING list's entry {i} has different size from column \"{attname}\"")
            };
            let tle_ty =
                backend_utils_adt_format_type::format_type_with_typemod(mcx, tletypid, tletypmod)?
                    .as_str()
                    .to_string();
            let col_ty = backend_utils_adt_format_type::format_type_with_typemod(
                mcx,
                attr.atttypid,
                attr.atttypmod,
            )?
            .as_str()
            .to_string();
            let detail = if is_select {
                format!("SELECT target entry has type {tle_ty}, but column has type {col_ty}.")
            } else {
                format!("RETURNING list entry has type {tle_ty}, but column has type {col_ty}.")
            };
            return Err(PgError::new(ERROR, msg)
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                .with_detail(detail));
        }
    }

    if i != natts {
        let msg = if is_select {
            "SELECT rule's target list has too few entries"
        } else {
            "RETURNING list has too few entries"
        };
        return Err(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
    }

    Ok(())
}

/* ===========================================================================
 * setRuleCheckAsUser (rewriteDefine.c:630)
 * ========================================================================= */

/// `setRuleCheckAsUser(node, userid)` — recursively set `checkAsUser` to
/// `userid` in all `RTEPermissionInfo`s of a query/expression tree.
pub fn setRuleCheckAsUser(node: &mut Node<'_>, userid: Oid) {
    setRuleCheckAsUser_walker(node, userid);
}

fn setRuleCheckAsUser_walker(node: &mut Node<'_>, userid: Oid) -> bool {
    if let Some(qry) = node.as_query_mut() {
        setRuleCheckAsUser_Query(qry, userid);
        return false;
    }
    backend_nodes_core::node_walker::expression_tree_walker_mut(node, &mut |child| {
        setRuleCheckAsUser_walker(child, userid)
    })
}

fn setRuleCheckAsUser_Query(qry: &mut Query<'_>, userid: Oid) {
    /* Set in all RTEPermissionInfos for this query. */
    for perminfo in qry.rteperminfos.iter_mut() {
        perminfo.checkAsUser = userid;
    }

    /* Now recurse to any subquery RTEs */
    for rte in qry.rtable.iter_mut() {
        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            if let Some(subquery) = rte.subquery.as_deref_mut() {
                setRuleCheckAsUser_Query(subquery, userid);
            }
        }
    }

    /* Recurse into subquery-in-WITH */
    for cte in qry.cteList.iter_mut() {
        if let Some(ctequery) = cte.as_query_mut() {
            setRuleCheckAsUser_Query(ctequery, userid);
        }
    }

    /* If there are sublinks, search for them and process their RTEs. */
    if qry.hasSubLinks {
        // query_tree_walker(qry, setRuleCheckAsUser_walker, &userid,
        //                   QTW_IGNORE_RC_SUBQUERIES);
        // QTW_IGNORE_RC_SUBQUERIES skips the rtable+CTE subqueries already
        // handled above; we still descend the expression trees to find SubLink
        // subselects (surfaced as `Node::Query`).
        let flags = backend_nodes_core::node_walker::QTW_IGNORE_RT_SUBQUERIES
            | backend_nodes_core::node_walker::QTW_IGNORE_CTE_SUBQUERIES;
        backend_nodes_core::node_walker::query_tree_mutator(
            qry,
            &mut |node| setRuleCheckAsUser_walker(node, userid),
            flags,
        );
    }
}

/* ===========================================================================
 * EnableDisableRule (rewriteDefine.c:691)
 * ========================================================================= */

/// `EnableDisableRule(rel, rulename, fires_when)` — change the firing
/// semantics of an existing rule.
pub fn EnableDisableRule<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    rulename: &str,
    fires_when: u8,
) -> PgResult<()> {
    let owning_rel = rel.rd_id;

    /* Find the rule tuple to change. */
    let pg_rewrite_desc =
        backend_access_table_table::table_open(mcx, RewriteRelationId, RowExclusiveLock)?;

    // ruletup = SearchSysCacheCopy2(RULERELNAME, owningRel, rulename);
    let ruletup =
        backend_utils_cache_syscache_seams::rule_tuple_by_relname::call(mcx, owning_rel, rulename)?;
    let Some((ruletup, ruleform)) = ruletup else {
        let relname = get_rel_name_or_empty(mcx, owning_rel)?;
        return Err(PgError::new(
            ERROR,
            format!("rule \"{rulename}\" for relation \"{relname}\" does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    };

    /* Verify that the user has appropriate permissions. */
    let event_relation_oid = ruleform.ev_class;
    debug_assert_eq!(event_relation_oid, owning_rel);
    let user_id = backend_utils_init_miscinit::GetUserId();
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        RelationRelationId,
        event_relation_oid,
        user_id,
    )? {
        let relkind = backend_utils_cache_lsyscache_seams::get_rel_relkind::call(event_relation_oid)?;
        let objtype = backend_catalog_objectaddress::resolve::get_relkind_objtype(relkind);
        let name = get_rel_name_or_empty(mcx, event_relation_oid)?;
        backend_catalog_aclchk_seams::aclcheck_error::call(ACLCHECK_NOT_OWNER, objtype, Some(name))?;
    }

    /* Change ev_enabled if it is different from the desired new state. */
    let mut changed = false;
    if ruleform.ev_enabled != fires_when {
        backend_catalog_indexing_seams::catalog_tuple_update_pg_rewrite_enabled::call(
            mcx,
            &pg_rewrite_desc,
            &ruletup,
            fires_when,
        )?;
        changed = true;
    }

    // InvokeObjectPostAlterHook(RewriteRelationId, ruleform->oid, 0);
    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        RewriteRelationId,
        ruleform.oid,
        0,
    )?;

    // heap_freetuple(ruletup); (the owned tuple drops here)
    pg_rewrite_desc.close(RowExclusiveLock)?;

    /* If we changed anything, broadcast an SI inval to rebuild relcache. */
    if changed {
        backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(rel)?;
    }

    Ok(())
}

/* ===========================================================================
 * RangeVarCallbackForRenameRule (rewriteDefine.c:755)
 * ========================================================================= */

/// `RangeVarCallbackForRenameRule` — permission + integrity checks before
/// acquiring the relation lock (the `RangeVarGetRelidExtended` callback).
fn RangeVarCallbackForRenameRule(mcx: Mcx<'_>, rv: &types_tuple::access::RangeVar, relid: Oid) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, relid); if invalid return (concurrently dropped);
    let class = backend_utils_cache_syscache_seams::class_relkind_namespace::call(relid)?;
    let Some((relkind, relnamespace)) = class else {
        return Ok(()); /* concurrently dropped */
    };
    let rv_relname = rv.relname.as_str();

    /* only tables and views can have rules */
    if relkind != RELKIND_RELATION && relkind != RELKIND_VIEW && relkind != RELKIND_PARTITIONED_TABLE
    {
        let err = PgError::new(ERROR, format!("relation \"{rv_relname}\" cannot have rules"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
        return Err(attach_relkind_detail(err, relkind)?);
    }

    // if (!allowSystemTableMods && IsSystemClass(relid, form)) ereport(ERROR, ...);
    if !backend_utils_init_small::globals::allowSystemTableMods()
        && backend_catalog_catalog::IsSystemClassByNamespace(relid, relnamespace)
    {
        return Err(PgError::new(
            ERROR,
            format!("permission denied: \"{rv_relname}\" is a system catalog"),
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    /* you must own the table to rename one of its rules */
    let user_id = backend_utils_init_miscinit::GetUserId();
    if !backend_catalog_aclchk_seams::object_ownercheck::call(RelationRelationId, relid, user_id)? {
        let objtype = backend_catalog_objectaddress::resolve::get_relkind_objtype(relkind);
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objtype,
            Some(rv_relname.to_string()),
        )?;
    }

    let _ = mcx;
    Ok(())
}

/* ===========================================================================
 * RenameRewriteRule (rewriteDefine.c:792)
 * ========================================================================= */

/// `RenameRewriteRule(relation, oldName, newName)` — rename an existing rewrite
/// rule.
pub fn RenameRewriteRule<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &types_tuple::access::RangeVar,
    old_name: &str,
    new_name: &str,
) -> PgResult<ObjectAddress> {
    /*
     * Look up name, check permissions, and acquire lock (held until end of
     * transaction).
     */
    let mut cb = |rv: &types_tuple::access::RangeVar, relid: Oid, _old_relid: Oid| {
        RangeVarCallbackForRenameRule(mcx, rv, relid)
    };
    let relid = backend_catalog_namespace::RangeVarGetRelidExtended(
        mcx,
        relation,
        AccessExclusiveLock,
        0,
        Some(&mut cb),
    )?;

    /* Have lock already, so just need to build relcache entry. */
    let targetrel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;

    /* Prepare to modify pg_rewrite */
    let pg_rewrite_desc =
        backend_access_table_table::table_open(mcx, RewriteRelationId, RowExclusiveLock)?;

    /* Fetch the rule's entry (it had better exist) */
    let ruletup =
        backend_utils_cache_syscache_seams::rule_tuple_by_relname::call(mcx, relid, old_name)?;
    let Some((ruletup, ruleform)) = ruletup else {
        return Err(PgError::new(
            ERROR,
            format!(
                "rule \"{old_name}\" for relation \"{}\" does not exist",
                targetrel.name()
            ),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    };
    let rule_oid = ruleform.oid;

    /* rule with the new name should not already exist */
    if backend_rewrite_rewritesupport_seams::IsDefinedRewriteRule::call(relid, new_name)? {
        return Err(PgError::new(
            ERROR,
            format!(
                "rule \"{new_name}\" for relation \"{}\" already exists",
                targetrel.name()
            ),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /*
     * We disallow renaming ON SELECT rules, because they should always be
     * named "_RETURN".  (ev_type stored as CmdType + '0'.)
     */
    if ruleform.ev_type == cmdtype_ev_type(CmdType::CMD_SELECT) {
        return Err(
            PgError::new(ERROR, "renaming an ON SELECT rule is not allowed")
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        );
    }

    /* OK, do the update: namestrcpy(&ruleform->rulename, newName) + update. */
    backend_catalog_indexing_seams::catalog_tuple_update_pg_rewrite_name::call(
        mcx,
        &pg_rewrite_desc,
        &ruletup,
        new_name,
    )?;

    // InvokeObjectPostAlterHook(RewriteRelationId, ruleOid, 0);
    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        RewriteRelationId,
        rule_oid,
        0,
    )?;

    // heap_freetuple(ruletup);
    pg_rewrite_desc.close(RowExclusiveLock)?;

    /* Invalidate relation's relcache entry (SI message). */
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(&targetrel)?;

    let address = ObjectAddress {
        classId: RewriteRelationId,
        objectId: rule_oid,
        objectSubId: 0,
    };

    /* Close rel, but keep exclusive lock! */
    targetrel.close(NoLock)?;

    Ok(address)
}
