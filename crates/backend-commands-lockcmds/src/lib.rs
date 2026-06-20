#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large error type shared across the whole tree, so boxing it
// would diverge from every sibling crate's `Result` shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/lockcmds.c` — `LOCK TABLE` command support code.
//!
//! lockcmds.c's own decision logic lives in-crate: the per-relation loop, the
//! `RELKIND_VIEW` / `recurse` dispatch, the pre-lock permission callback (skip
//! on `InvalidOid` / concurrently-dropped, reject a non-lockable relkind, note
//! temp-namespace access, run the ACL check), the inheritance recursion (honor
//! `nowait`, skip children dropped out from under us and release their useless
//! lock), the view range-table walk (lock every referenced table/view, with the
//! self-reference guard, checking permission as the view owner or current user),
//! and the lock-mode → privilege-mask translation.
//!
//! Genuine cross-subsystem callees cross seams into their owners
//! (`get_rel_relkind` / `get_rel_persistence` / `get_rel_name` → lsyscache,
//! `pg_class_aclcheck` / `aclcheck_error` → aclchk, `GetUserId` → miscinit,
//! `MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE` → xact,
//! `find_all_inheritors` → pg_inherits, `LockRelationOid` /
//! `ConditionalLockRelationOid` / `UnlockRelationOid` → lmgr,
//! `SearchSysCacheExists1(RELOID)` → syscache, `table_open` → table,
//! `get_view_query` → rewriteHandler, `get_relkind_objtype` → objectaddress).
//! `RangeVarGetRelidExtended` (the resolve-with-callback that runs the
//! permission gate mid-lookup) is called directly into the ported
//! `backend-catalog-namespace`, and `query_tree_walker` /
//! `expression_tree_walker` directly into `backend-nodes-core`.
//!
//! ## Function inventory (lockcmds.c, PostgreSQL 18.3 — 6 functions)
//!
//! * `LockTableCommand`                — C 40-64
//! * `RangeVarCallbackForLockTable`    — C 70-107 (static)
//! * `LockTableRecurse`                — C 116-158 (static)
//! * `LockViewRecurse_walker`          — C 178-242 (static)
//! * `LockViewRecurse`                 — C 244-274 (static)
//! * `LockTableAclCheck`               — C 279-299 (static)

use backend_utils_error::ereport;
use mcx::{Mcx, MemoryContext};

use types_acl::acl::{
    AclMode, AclResult, ACLCHECK_OK, ACL_DELETE, ACL_INSERT, ACL_MAINTAIN, ACL_SELECT,
    ACL_TRUNCATE, ACL_UPDATE,
};
use types_core::primitive::{Oid, OidIsValid};
use types_core::catalog::RELPERSISTENCE_TEMP;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::ddlnodes::LockStmt;
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_tuple::access::RangeVar;
use types_storage::lock::{AccessShareLock, RowExclusiveLock, NoLock, LOCKMODE};
use types_tuple::access::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW};

use backend_nodes_core::node_walker::{
    expression_tree_walker, query_tree_walker, QTW_IGNORE_JOINALIASES,
};

use backend_catalog_objectaddress_seams::get_relkind_objtype;
use backend_catalog_pg_class::errdetail_relkind_not_supported;

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_pg_inherits_seams as pg_inherits;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_access_transam_xact_seams as xact;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_access_table_table_seams as table;
use backend_rewrite_rewritehandler_seams as rewrite;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("lockcmds.c", 0, funcname)
}

/// Install this unit's inward seam.
pub fn init_seams() {
    backend_commands_lockcmds_seams::lock_table_command::set(lock_table_command);
    // The tcop/utility.c dispatch routes LOCK TABLE through its own outward
    // seam; install the real arm here (the owner crate).
    backend_tcop_utility_out_seams::lock_table_command::set(lock_table_command_arm);
}

/// `case T_LockStmt:` (utility.c) — LOCK TABLE.
fn lock_table_command_arm(parsetree: &Node) -> PgResult<()> {
    let Some(lockstmt) = parsetree.as_lockstmt() else {
        panic!("lock_table_command: parse tree is not a LockStmt");
    };
    lock_table_command(lockstmt)
}

/* =========================================================================
 * LOCK TABLE  (C lines 40-64)
 * ========================================================================= */

/// `LockTableCommand(LockStmt *lockstmt)` — iterate over the named relations and
/// process them one at a time.
pub fn lock_table_command(lockstmt: &LockStmt) -> PgResult<()> {
    // The command works in the current memory context (C
    // `CurrentMemoryContext`); a per-call context stands in here for the
    // transient catalog/name-resolution copies the resolution seams make.
    let ctx = MemoryContext::new("LockTableCommand");
    let mcx = ctx.mcx();

    /*
     * Iterate over the list and process the named relations one at a time
     */
    for p in lockstmt.relations.iter() {
        // RangeVar *rv = (RangeVar *) lfirst(p);
        let rv: RangeVar = match p.as_rangevar() {
            Some(rv) => to_access_range_var(rv),
            None => panic!(
                "LockTableCommand: LockStmt.relations element is not a RangeVar (tag {:?})",
                p.node_tag()
            ),
        };
        let recurse = rv.inh; /* bool recurse = rv->inh; */

        /*
         * reloid = RangeVarGetRelidExtended(rv, lockstmt->mode,
         *                                   lockstmt->nowait ? RVR_NOWAIT : 0,
         *                                   RangeVarCallbackForLockTable,
         *                                   &lockstmt->mode);
         *
         * The lockcmds callback runs inside the lookup (after name resolution,
         * before/around locking); its only state, the lock mode (`&lockstmt->mode`
         * in C), is captured by the closure so the resolver invokes the gate at
         * the same point.
         */
        let mode = lockstmt.mode;
        let flags = if lockstmt.nowait {
            types_namespace::RVR_NOWAIT
        } else {
            0
        };
        let mut callback = |rv: &RangeVar, relid: Oid, old_relid: Oid| -> PgResult<()> {
            range_var_callback_for_lock_table(rv, relid, old_relid, mode)
        };
        let reloid: Oid = backend_catalog_namespace::RangeVarGetRelidExtended(
            mcx,
            &rv,
            lockstmt.mode,
            flags,
            Some(&mut callback),
        )?;

        if lsyscache::get_rel_relkind::call(reloid)? == RELKIND_VIEW {
            lock_view_recurse(mcx, reloid, lockstmt.mode, lockstmt.nowait, Vec::new())?;
        } else if recurse {
            lock_table_recurse(mcx, reloid, lockstmt.mode, lockstmt.nowait)?;
        }
    }

    Ok(())
}

/* =========================================================================
 * RangeVarCallbackForLockTable  (C lines 70-107)
 * ========================================================================= */

/// `RangeVarCallbackForLockTable(const RangeVar *rv, Oid relid, Oid oldrelid,
/// void *arg)` — before acquiring a table lock on the named table, check whether
/// we have permission to do so.  `arg` is `*(LOCKMODE *) arg`, threaded here as
/// the explicit `lockmode` parameter.
fn range_var_callback_for_lock_table(
    rv: &RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if !OidIsValid(relid) {
        return Ok(()); /* doesn't exist, so no permissions check */
    }
    let relkind: u8 = lsyscache::get_rel_relkind::call(relid)?;
    if relkind == 0 {
        return Ok(()); /* woops, concurrently dropped; no permissions check */
    }

    /* Currently, we only allow plain tables or views to be locked */
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_PARTITIONED_TABLE
        && relkind != RELKIND_VIEW
    {
        let detail = errdetail_relkind_not_supported(relkind)?;
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("cannot lock relation \"{}\"", rv.relname))
            .errdetail(detail)
            .finish(here("RangeVarCallbackForLockTable"));
    }

    /*
     * Make note if a temporary relation has been accessed in this transaction.
     */
    let relpersistence: u8 = lsyscache::get_rel_persistence::call(relid)?;
    if relpersistence == RELPERSISTENCE_TEMP {
        xact::set_xact_accessed_temp_namespace::call(); /* MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE */
    }

    /* Check permissions. */
    let aclresult = lock_table_acl_check(relid, lockmode, miscinit::get_user_id::call())?;
    if aclresult != ACLCHECK_OK {
        aclchk::aclcheck_error::call(
            aclresult,
            get_relkind_objtype::call(lsyscache::get_rel_relkind::call(relid)?),
            Some(rv.relname.clone()),
        )?;
    }

    Ok(())
}

/* =========================================================================
 * LockTableRecurse  (C lines 116-158)
 * ========================================================================= */

/// `LockTableRecurse(Oid reloid, LOCKMODE lockmode, bool nowait)` — apply LOCK
/// TABLE recursively over an inheritance tree.
///
/// This doesn't check permission to perform LOCK TABLE on the child tables,
/// because getting here means that the user has permission to lock the parent
/// which is enough.
fn lock_table_recurse(
    mcx: Mcx<'_>,
    reloid: Oid,
    lockmode: LOCKMODE,
    nowait: bool,
) -> PgResult<()> {
    /* children = find_all_inheritors(reloid, NoLock, NULL); */
    let children = pg_inherits::find_all_inheritors::call(mcx, reloid, NoLock)?;

    for childreloid in children.iter().copied() {
        /* Parent already locked. */
        if childreloid == reloid {
            continue;
        }

        let guard;
        if !nowait {
            guard = lmgr::lock_relation_oid::call(childreloid, lockmode)?;
        } else {
            match lmgr::conditional_lock_relation_oid::call(childreloid, lockmode)? {
                Some(g) => guard = g,
                None => {
                    /* try to throw error by name; relation could be deleted... */
                    let Some(relname) = lsyscache::get_rel_name::call(mcx, childreloid)? else {
                        continue; /* child concurrently dropped, just skip it */
                    };
                    return ereport(ERROR)
                        .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                        .errmsg(format!(
                            "could not obtain lock on relation \"{}\"",
                            relname.as_str()
                        ))
                        .finish(here("LockTableRecurse"));
                }
            }
        }

        /*
         * Even if we got the lock, child might have been concurrently dropped.
         * If so, we can skip it.
         */
        if !syscache::search_syscache_exists_reloid::call(childreloid)? {
            /* Release useless lock */
            guard.release()?; /* UnlockRelationOid(childreloid, lockmode) */
            continue;
        }

        /* Hold the lock until transaction end (the C default). */
        guard.keep();
    }

    Ok(())
}

/* =========================================================================
 * LockViewRecurse_context + LockViewRecurse_walker  (C lines 167-242)
 * ========================================================================= */

/// `LockViewRecurse_context` — state threaded through the view range-table walk.
struct LockViewRecurseContext {
    /// lock mode to use
    lockmode: LOCKMODE,
    /// no wait mode
    nowait: bool,
    /// user for checking the privilege
    check_as_user: Oid,
    /// OID of the view to be locked
    #[allow(dead_code)]
    viewoid: Oid,
    /// OIDs of ancestor views
    ancestor_views: Vec<Oid>,
}

/// `LockViewRecurse_walker(Node *node, LockViewRecurse_context *context)` —
/// processes a `Query`'s range table (locking each referenced table/view) then
/// recurses into the query's expression subtrees; on any other node it just
/// recurses.  Returns `Ok(true)` to abort the walk (C's `return true`).
///
/// The C callback may `ereport(ERROR)` mid-walk; here that is a `PgResult`
/// error.  Because the [`backend_nodes_core`] walker callback can only return
/// `bool`, this surfaces a `PgResult` directly and the closure that bridges to
/// the generic walker (in [`walk_via`]) stashes any error and aborts.
fn lock_view_recurse_walker(
    mcx: Mcx<'_>,
    node: &Node,
    context: &mut LockViewRecurseContext,
) -> PgResult<bool> {
    if let Some(query) = node.as_query() {
        for rte in query.rtable.iter() {
            let relid: Oid = rte.relid;
            let relkind: u8 = rte.relkind as u8;
            let relname = lsyscache::get_rel_name::call(mcx, relid)?;

            /* Currently, we only allow plain tables or views to be locked. */
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_PARTITIONED_TABLE
                && relkind != RELKIND_VIEW
            {
                continue;
            }

            /*
             * We might be dealing with a self-referential view.  If so, we can
             * just stop recursing, since we already locked it.
             */
            if context.ancestor_views.contains(&relid) {
                continue;
            }

            /*
             * Check permissions as the specified user.  This will either be the
             * view owner or the current user.
             */
            let aclresult = lock_table_acl_check(relid, context.lockmode, context.check_as_user)?;
            if aclresult != ACLCHECK_OK {
                aclchk::aclcheck_error::call(
                    aclresult,
                    get_relkind_objtype::call(relkind),
                    relname.as_ref().map(|s| s.as_str().to_string()),
                )?;
            }

            /* We have enough rights to lock the relation; do so. */
            if !context.nowait {
                lmgr::lock_relation_oid::call(relid, context.lockmode)?.keep();
            } else {
                match lmgr::conditional_lock_relation_oid::call(relid, context.lockmode)? {
                    Some(g) => g.keep(),
                    None => {
                        return ereport(ERROR)
                            .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                            .errmsg(format!(
                                "could not obtain lock on relation \"{}\"",
                                relname.as_ref().map(|s| s.as_str()).unwrap_or_default()
                            ))
                            .finish(here("LockViewRecurse_walker"))
                            .map(|()| false);
                    }
                }
            }

            if relkind == RELKIND_VIEW {
                lock_view_recurse(
                    mcx,
                    relid,
                    context.lockmode,
                    context.nowait,
                    context.ancestor_views.clone(),
                )?;
            } else if rte.inh {
                lock_table_recurse(mcx, relid, context.lockmode, context.nowait)?;
            }
        }

        /*
         * return query_tree_walker(query, LockViewRecurse_walker, context,
         *                          QTW_IGNORE_JOINALIASES);
         */
        return walk_via(mcx, context, |walker| {
            query_tree_walker(query, walker, QTW_IGNORE_JOINALIASES)
        });
    }

    /* return expression_tree_walker(node, LockViewRecurse_walker, context); */
    walk_via(mcx, context, |walker| expression_tree_walker(node, walker))
}

/// Bridge the `PgResult`-returning [`lock_view_recurse_walker`] to the generic
/// `bool`-returning [`backend_nodes_core`] walker.
///
/// `recurse` runs the chosen generic walker with a closure that re-enters
/// [`lock_view_recurse_walker`] on each child node, stashing any error and
/// aborting (returning `true`) on the first failure.  After the walk returns we
/// surface a stashed error (if any) or the walker's abort flag — exactly C's
/// `return query_tree_walker(...)` / `return expression_tree_walker(...)` whose
/// `ereport(ERROR)` non-local exits become this propagated `PgResult`.
fn walk_via(
    mcx: Mcx<'_>,
    context: &mut LockViewRecurseContext,
    recurse: impl FnOnce(&mut dyn FnMut(&Node) -> bool) -> bool,
) -> PgResult<bool> {
    let mut err: Option<PgError> = None;
    let aborted = {
        let err_slot = &mut err;
        let mut walker = |child: &Node| -> bool {
            if err_slot.is_some() {
                return true;
            }
            match lock_view_recurse_walker(mcx, child, context) {
                Ok(abort) => abort,
                Err(e) => {
                    *err_slot = Some(e);
                    true
                }
            }
        };
        recurse(&mut walker)
    };

    if let Some(e) = err {
        return Err(e);
    }
    Ok(aborted)
}

/* =========================================================================
 * LockViewRecurse  (C lines 244-274)
 * ========================================================================= */

/// `LockViewRecurse(Oid reloid, LOCKMODE lockmode, bool nowait,
/// List *ancestor_views)` — apply LOCK TABLE recursively over a view.
///
/// All tables and views appearing in the view definition query are locked
/// recursively with the same lock mode.
fn lock_view_recurse(
    mcx: Mcx<'_>,
    reloid: Oid,
    lockmode: LOCKMODE,
    nowait: bool,
    ancestor_views: Vec<Oid>,
) -> PgResult<()> {
    /* caller has already locked the view */
    let view = table::table_open::call(mcx, reloid, NoLock)?;
    let viewquery: Query = rewrite::get_view_query::call(mcx, &view)?;

    /*
     * If the view has the security_invoker property set, check permissions as
     * the current user.  Otherwise, check permissions as the view owner.
     */
    let check_as_user = if rewrite::relation_has_security_invoker::call(&view) {
        miscinit::get_user_id::call()
    } else {
        view.rd_rel.relowner /* view->rd_rel->relowner */
    };
    /* context.ancestor_views = lappend_oid(ancestor_views, reloid); */
    let mut ancestor_views = ancestor_views;
    ancestor_views
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
    ancestor_views.push(reloid);

    let mut context = LockViewRecurseContext {
        lockmode,
        nowait,
        check_as_user,
        viewoid: reloid,
        ancestor_views,
    };

    /* LockViewRecurse_walker((Node *) viewquery, &context); */
    let node = Node::mk_query(mcx, viewquery);
    let walk_result = lock_view_recurse_walker(mcx, &node, &mut context);

    /*
     * context.ancestor_views = list_delete_last(context.ancestor_views);
     *
     * Performed before propagating any walker error so the bookkeeping matches
     * C (the list is function-local and discarded, but the delete is kept for
     * fidelity).
     */
    context.ancestor_views.pop();

    walk_result?;

    /* table_close(view, NoLock); */
    view.close(NoLock)?;

    Ok(())
}

/* =========================================================================
 * LockTableAclCheck  (C lines 279-299)
 * ========================================================================= */

/// `LockTableAclCheck(Oid reloid, LOCKMODE lockmode, Oid userid)` — check whether
/// the current user is permitted to lock this relation.
fn lock_table_acl_check(reloid: Oid, lockmode: LOCKMODE, userid: Oid) -> PgResult<AclResult> {
    /* any of these privileges permit any lock mode */
    let mut aclmask: AclMode = ACL_MAINTAIN | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE;

    /* SELECT privileges also permit ACCESS SHARE and below */
    if lockmode <= AccessShareLock {
        aclmask |= ACL_SELECT;
    }

    /* INSERT privileges also permit ROW EXCLUSIVE and below */
    if lockmode <= RowExclusiveLock {
        aclmask |= ACL_INSERT;
    }

    let aclresult = aclchk::pg_class_aclcheck::call(reloid, userid, aclmask)?;

    Ok(aclresult)
}

/// `(RangeVar *) lfirst(p)` — re-encode a parse-node `RangeVar` (the
/// arena-lifetimed `types_nodes::rawnodes::RangeVar`) into the
/// `RangeVarGetRelidExtended` argument type (`types_tuple::access::RangeVar`,
/// the trimmed name-only view the namespace resolver and its callback consume).
/// Mirrors the conversion `backend-parser-relation` performs for the same
/// resolver entry point.
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> RangeVar {
    RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

#[cfg(test)]
mod tests;
